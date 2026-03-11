import json
import random
import re
from datetime import datetime
from typing import Dict, List, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from categories.kalcer_categories import KALCER_CATEGORIES
from common.http_utils import (
    DEFAULT_USER_AGENTS,
    build_session,
    get_page_content,
    warmup_session,
)
from common.logging_utils import open_logger
from common.paths import create_output_paths
from common.price_utils import (
    convert_price_to_without_vat,
    extract_all_prices,
    extract_price_per_unit,
)
from common.runtime_utils import batch_pause, startup_sleep
from common.save_utils import (
    get_max_zap,
    load_existing_data,
    save_data_batch_json_only,
    write_excel_from_json,
)
from common.schema import (
    build_excel_columns,
    get_base_record,
    merge_extra_columns_from_data,
)
from common.text_utils import (
    clean_multiline_text,
    clean_text,
    extract_ean_raw,
    safe_truncate,
    unique_preserve_order,
)
from common.unit_utils import guess_em_from_text, normalize_em


SHOP_NAME = "Kalcer"
BASE_URL = "https://www.trgovina-kalcer.si"
DDV_RATE = 0.22
BATCH_SIZE = 30


def build_page_url(category_url: str, page: int) -> str:
    sep = "&" if "?" in category_url else "?"
    return f"{category_url}{sep}page={page}"


NON_PRODUCT_PATTERNS = (
    "/account/",
    "/checkout/",
    "/information/",
    "/download/",
    "/product/search",
    "/product/manufacturer",
    "/blog/",
    "/image/",
    "facebook.com",
    "instagram.com",
    "mailto:",
    "javascript:",
    "#",
)


def is_probable_product_url(url: str) -> bool:
    if not url or not url.startswith(BASE_URL):
        return False

    lower = url.lower().strip()

    if any(pattern in lower for pattern in NON_PRODUCT_PATTERNS):
        return False

    path = lower.replace(BASE_URL.lower(), "", 1).strip("/")
    if not path:
        return False

    parts = [p for p in path.split("/") if p]
    if len(parts) != 1:
        return False

    blocked_roots = {
        "gradnja",
        "stukature",
        "kopalnica-wellness",
        "vrata-okna-stopnice",
        "orodja",
        "blog",
        "kontakt",
        "splosni-pogoji",
        "vracilo-blaga-kalcer",
        "dostava-nacini-placila",
        "o-kalcer",
        "varovanje-podatkov-zasebnost",
        "izjava-o-dostopnosti",
    }

    return parts[0] not in blocked_roots


def extract_product_links_from_category_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []

    selectors = [
        ".product-layout .image a",
        ".product-thumb .image a",
        ".product-grid .image a",
        ".product-list .image a",
        ".caption h4 a",
        ".name a",
    ]

    for selector in selectors:
        for a_tag in soup.select(selector):
            href = clean_text(a_tag.get("href"))
            if not href:
                continue

            full_url = urljoin(BASE_URL, href)
            if is_probable_product_url(full_url):
                links.append(full_url)

    return unique_preserve_order(links)


def get_product_links_from_category(session, user_agent: str, category_url: str, logger) -> List[str]:
    all_links: List[str] = []
    seen_first_links: set[str] = set()
    page = 1

    while True:
        page_url = build_page_url(category_url, page)
        logger.log(f"  Stran {page}: {page_url}")

        html = get_page_content(
            session=session,
            url=page_url,
            base_url=BASE_URL,
            user_agent=user_agent,
            referer=category_url,
            timeout=25,
            retries=3,
            sleep_min=1.1,
            sleep_max=2.8,
            logger=logger,
        )

        if not html:
            break

        page_links = extract_product_links_from_category_html(html)
        if not page_links:
            break

        first_link = page_links[0]
        if first_link in seen_first_links:
            logger.log(f"  Stran {page} se ponavlja, zaključujem kategorijo.")
            break

        seen_first_links.add(first_link)

        new_count = 0
        for link in page_links:
            if link not in all_links:
                all_links.append(link)
                new_count += 1

        if new_count == 0:
            break

        page += 1

    return all_links


def extract_main_image(soup: BeautifulSoup) -> str:
    og_image = soup.select_one('meta[property="og:image"]')
    if og_image and og_image.get("content"):
        return clean_text(og_image.get("content"))

    selectors = [
        ".thumbnails a[href]",
        ".image-additional a[href]",
        ".product-info .image a[href]",
        "a.lightbox-image[href]",
        ".product-info img[src]",
        ".thumbnail img[src]",
    ]

    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            href = node.get("href") or node.get("src")
            if href:
                return urljoin(BASE_URL, clean_text(href))

    return ""


def extract_price_block_text(soup: BeautifulSoup) -> str:
    selectors = [
        ".product-price",
        ".price",
        ".product-info .price",
        ".price-box",
    ]

    parts: List[str] = []

    for selector in selectors:
        for node in soup.select(selector):
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                parts.append(text)

    return " | ".join(unique_preserve_order(parts))


def extract_prices_and_em(soup: BeautifulSoup, title: str) -> Tuple[str, str, str, str, str]:
    price_text = extract_price_block_text(soup)

    price_per_unit, unit = extract_price_per_unit(price_text)
    unit = normalize_em(unit) if unit else ""

    all_prices = extract_all_prices(price_text)

    regular_price = ""
    sale_price = ""

    if price_per_unit:
        regular_price = price_per_unit
    elif all_prices:
        regular_price = all_prices[0]

    # Kalcer pogosto kaže:
    # od 39,44€ (4,21€/M2)
    # Brez DDV: 32,33€
    # Redna cena (z DDV): 81,32€
    #
    # V takem primeru želimo:
    # Cena / EM = 4,21
    # Akcijska cena prazno
    #
    # Če ni cene na EM, pa vzamemo glavno ceno.
    #
    # Ne poskušamo na silo mapirati "Redna cena (z DDV)" v akcijsko ceno na EM.

    if not unit:
        title_em = normalize_em(guess_em_from_text(title))
        unit = title_em if title_em else "kos"

    if not regular_price:
        regular_price = ""

    return (
        regular_price,
        sale_price,
        convert_price_to_without_vat(regular_price, DDV_RATE),
        convert_price_to_without_vat(sale_price, DDV_RATE),
        unit or "kos",
    )


def extract_manufacturer_from_soup(soup: BeautifulSoup) -> str:
    text = soup.get_text("\n", strip=True)
    match = re.search(r"Proizvajalec\s*:\s*(.+)", text)
    if not match:
        return ""

    value = clean_text(match.group(1))
    value = re.split(r"\b(Šifra|EAN|Izberite|Količina|Opis)\b", value, maxsplit=1)[0].strip()
    return clean_text(value)


def extract_sku_from_soup(soup: BeautifulSoup) -> str:
    text = soup.get_text("\n", strip=True)
    match = re.search(r"Šifra\s*:\s*([^\n\r]+)", text)
    return clean_text(match.group(1)) if match else ""


def extract_description_text(soup: BeautifulSoup) -> str:
    # Prioriteta: vsebina zavihka Opis
    possible_nodes = []

    for selector in [
        "#tab-description",
        ".tab-content",
        ".product-tabs-content",
        ".product-description",
    ]:
        for node in soup.select(selector):
            txt = clean_multiline_text(node.get_text("\n", strip=True))
            if txt and len(txt) > 80:
                possible_nodes.append(txt)

    if possible_nodes:
        cleaned = max(possible_nodes, key=len)
        cleaned = re.sub(r"\bNapišite mnenje\b.*", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"\bDodaj na seznam želja\b.*", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"\bPrimerjaj ta izdelek\b.*", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"\bKoličina\b.*", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
        return safe_truncate(cleaned.strip())

    # Fallback: poišči sekcijo po headingu "Opis"
    heading = soup.find(["h2", "h3"], string=re.compile(r"^\s*Opis\s*$", flags=re.IGNORECASE))
    if heading:
        texts: List[str] = []
        for sibling in heading.find_all_next():
            if sibling.name in {"h1", "h2", "h3", "h4"} and sibling is not heading:
                break
            text = clean_text(sibling.get_text(" ", strip=True))
            if text:
                texts.append(text)

        if texts:
            cleaned = clean_multiline_text("\n".join(unique_preserve_order(texts)))
            cleaned = re.sub(r"\bNapišite mnenje\b.*", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
            cleaned = re.sub(r"\bDodaj na seznam želja\b.*", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
            cleaned = re.sub(r"\bPrimerjaj ta izdelek\b.*", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
            return safe_truncate(cleaned.strip())

    return ""


def parse_stock_data(soup: BeautifulSoup):
    centers: Dict[str, str] = {}
    page_text = soup.get_text("\n", strip=True)

    if "Za prikaz zaloge izberite možnosti" in page_text:
        return "", "", {}

    rows = soup.select(".listing.stockMargin tr")
    for row in rows:
        cells = row.select("td")
        if len(cells) >= 2:
            center = clean_text(cells[0].get_text(" ", strip=True))
            value = clean_text(cells[1].get_text(" ", strip=True))

            if center and value and center.lower() not in {"ident", "enota mere"}:
                centers[center] = value

    if not centers:
        fallback_matches = re.findall(
            r"(Ljubljana|Maribor|Novo Mesto|Celje|Koper|Trzin)\s*[:\-]\s*(DA|NE|\d+)",
            page_text,
            flags=re.IGNORECASE,
        )
        for center, value in fallback_matches:
            centers[clean_text(center)] = clean_text(value).upper()

    if centers:
        has_positive = False
        for value in centers.values():
            if value.upper() == "DA":
                has_positive = True
                break

            number_match = re.search(r"\d+", value)
            if number_match and int(number_match.group(0)) > 0:
                has_positive = True
                break

        delivery = "DA" if has_positive else "NE"
        return delivery, json.dumps(centers, ensure_ascii=False), centers

    delivery_match = re.search(r"(\d+\s*[-–]\s*\d+\s*delovnih\s*dni)", page_text, flags=re.IGNORECASE)
    if delivery_match:
        return clean_text(delivery_match.group(1)), "", {}

    if re.search(r"\bna zalogi\b", page_text, flags=re.IGNORECASE):
        return "DA", "", {}

    if re.search(r"\bni na zalogi\b", page_text, flags=re.IGNORECASE):
        return "NE", "", {}

    return "", "", {}


def extract_variant_options(soup: BeautifulSoup) -> List[str]:
    variants: List[str] = []

    for select in soup.select("select"):
        label = ""
        select_id = select.get("id")

        if select_id:
            label_node = soup.select_one(f'label[for="{select_id}"]')
            if label_node:
                label = clean_text(label_node.get_text(" ", strip=True))

        if not label:
            prev = select.find_previous(["label", "strong", "b", "h4"])
            if prev:
                label = clean_text(prev.get_text(" ", strip=True))

        options = []
        for option in select.select("option"):
            value = clean_text(option.get_text(" ", strip=True))
            if not value or value.lower() in {"izberite", "---", "opcijsko", "please select"}:
                continue
            options.append(value)

        if label and options:
            for option in options:
                variants.append(f"{label}: {option}")

    if not variants:
        page_text = soup.get_text("\n", strip=True)
        match = re.search(r"Izberite:\s*(.+?)\s*Količina", page_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            block = clean_multiline_text(match.group(1))
            lines = [line.strip() for line in block.splitlines() if line.strip()]
            if len(lines) >= 2:
                label = lines[0]
                for value in lines[1:]:
                    if value.lower() == label.lower():
                        continue
                    variants.append(f"{label}: {value}")

    return unique_preserve_order(variants)


def build_records_for_product(
    url: str,
    category_slug: str,
    today_str: str,
    soup: BeautifulSoup,
    next_zap_start: int,
):
    title_node = soup.select_one("h1")
    title = clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
    if not title:
        return [], next_zap_start

    sku = extract_sku_from_soup(soup)
    manufacturer = extract_manufacturer_from_soup(soup)
    ean = extract_ean_raw(soup.get_text("\n", strip=True))
    description = extract_description_text(soup)
    image_url = extract_main_image(soup)

    dobava, stock_json, centers = parse_stock_data(soup)

    variants = extract_variant_options(soup)

    price_w_vat, sale_w_vat, price_wo_vat, sale_wo_vat, em = extract_prices_and_em(
        soup,
        title,
    )

    base_row = get_base_record()
    base_row.update(
        {
            "Skupina": category_slug,
            "Opis": title,
            "Opis izdelka": description,
            "Oznaka / naziv": sku,
            "EAN": ean,
            "EM": normalize_em(em) if em else "kos",
            "Proizvajalec": manufacturer,
            "Veljavnost od": today_str,
            "Dobava": dobava,
            "Zaloga po centrih": stock_json,
            "Cena / EM (z DDV)": price_w_vat,
            "Akcijska cena / EM (z DDV)": sale_w_vat,
            "Cena / EM (brez DDV)": price_wo_vat,
            "Akcijska cena / EM (brez DDV)": sale_wo_vat,
            "URL": url,
            "SLIKA URL": image_url,
        }
    )

    for center_name, value in centers.items():
        base_row[f"Zaloga - {center_name}"] = value

    records: List[Dict[str, str]] = []

    if variants:
        for variant in variants:
            row = dict(base_row)
            row["Varianta"] = variant
            row["Zap"] = next_zap_start
            records.append(row)
            next_zap_start += 1
    else:
        row = dict(base_row)
        row["Zap"] = next_zap_start
        records.append(row)
        next_zap_start += 1

    return records, next_zap_start


def get_product_details(
    session,
    user_agent: str,
    url: str,
    category_slug: str,
    today_str: str,
    next_zap_start: int,
    logger,
):
    logger.log(f"    - Detajli: {url}")

    html = get_page_content(
        session=session,
        url=url,
        base_url=BASE_URL,
        user_agent=user_agent,
        referer=BASE_URL,
        timeout=25,
        retries=3,
        sleep_min=1.2,
        sleep_max=2.9,
        logger=logger,
    )

    if not html:
        return [], next_zap_start

    soup = BeautifulSoup(html, "html.parser")
    return build_records_for_product(url, category_slug, today_str, soup, next_zap_start)


def scrape_kalcer() -> Tuple[str, str, str]:
    startup_sleep()

    json_path, excel_path, log_path, _ = create_output_paths(SHOP_NAME)
    logger = open_logger(log_path)

    try:
        logger.log(f"--- Zagon {SHOP_NAME} ---")
        logger.log(f"JSON:  {json_path}")
        logger.log(f"Excel: {excel_path}")
        logger.log(f"Log:   {log_path}")

        existing_data = load_existing_data(json_path, excel_path)
        next_zap = get_max_zap(existing_data) + 1
        today_str = datetime.now().strftime("%Y-%m-%d")

        session = build_session()
        user_agent = random.choice(DEFAULT_USER_AGENTS)
        warmup_session(session, BASE_URL, user_agent)

        batch_rows: List[Dict[str, str]] = []
        processed_products = 0

        for group_name, category_urls in KALCER_CATEGORIES.items():
            logger.log("")
            logger.log(f"--- {group_name} ---")

            for category_url in category_urls:
                category_slug = category_url.rstrip("/").split("/")[-1]
                logger.log("")
                logger.log(f"  Kategorija: {category_slug}")

                product_links = get_product_links_from_category(
                    session,
                    user_agent,
                    category_url,
                    logger,
                )

                if not product_links:
                    logger.log("  Ni najdenih produktnih linkov.")
                    continue

                for product_url in product_links:
                    rows, next_zap = get_product_details(
                        session,
                        user_agent,
                        product_url,
                        category_slug,
                        today_str,
                        next_zap,
                        logger,
                    )

                    if rows:
                        batch_rows.extend(rows)
                        processed_products += 1

                    if len(batch_rows) >= BATCH_SIZE:
                        save_data_batch_json_only(
                            new_data=batch_rows,
                            json_path=json_path,
                            excel_path=excel_path,
                            logger=logger,
                            use_variant=True,
                        )
                        batch_rows = []

                    batch_pause(
                        processed_count=processed_products,
                        every_n=40,
                        pause_min=8.0,
                        pause_max=18.0,
                        logger=logger,
                    )

        if batch_rows:
            save_data_batch_json_only(
                new_data=batch_rows,
                json_path=json_path,
                excel_path=excel_path,
                logger=logger,
                use_variant=True,
            )

        final_data = load_existing_data(json_path, excel_path)
        columns = build_excel_columns(merge_extra_columns_from_data(final_data))
        write_excel_from_json(json_path, excel_path, columns, logger=logger)

        logger.log(f"Konec. Skupno zapisov: {len(final_data)}")
        return json_path, excel_path, log_path

    finally:
        logger.close()


if __name__ == "__main__":
    scrape_kalcer()
