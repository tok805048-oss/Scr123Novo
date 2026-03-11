import json
import random
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from categories.merkur_categories import MERKUR_CATEGORIES
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
    extract_first_price,
    round_price_2dec,
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
from common.text_utils import clean_multiline_text, clean_text, extract_ean_raw, safe_truncate
from common.unit_utils import guess_em_from_text, normalize_em


SHOP_NAME = "Merkur"
BASE_URL = "https://www.merkur.si"
DDV_RATE = 0.22
BATCH_SIZE = 30
MAX_PAGES = 250


def add_or_replace_query(url: str, params: Dict[str, str]) -> str:
    parts = urlparse(url)
    q = dict(parse_qsl(parts.query))
    q.update(params)
    new_query = urlencode(q)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def build_page_url(category_url: str, page: int) -> str:
    return add_or_replace_query(category_url, {"p": str(page)}) + "#section-products"


def extract_product_links_from_category_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []

    container = soup.find("div", class_="list-items")
    if not container:
        return links

    items = container.find_all("div", class_="item")
    for item in items:
        a_tag = item.find("a", href=True)
        if not a_tag:
            continue

        href = clean_text(a_tag.get("href"))
        if not href:
            continue

        full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
        if full_url.startswith(BASE_URL):
            links.append(full_url)

    return list(dict.fromkeys(links))


def get_product_links_from_category(session, user_agent: str, category_url: str, logger) -> List[str]:
    all_links: List[str] = []
    seen_first_items: set[str] = set()

    for page in range(1, MAX_PAGES + 1):
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
            sleep_min=1.0,
            sleep_max=2.8,
            logger=logger,
        )
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        container = soup.find("div", class_="list-items")
        if not container:
            break

        items = container.find_all("div", class_="item")
        if not items:
            break

        first_title = clean_text(items[0].get_text(" ", strip=True))[:120]
        if page > 1 and first_title and first_title in seen_first_items:
            logger.log("  Stran se ponavlja. Konec kategorije.")
            break

        if first_title:
            seen_first_items.add(first_title)

        page_links = extract_product_links_from_category_html(html)
        if not page_links:
            break

        new_count = 0
        for link in page_links:
            if link not in all_links:
                all_links.append(link)
                new_count += 1

        if new_count == 0:
            break

        if not soup.select_one("a.next"):
            break

    return all_links


def extract_main_image(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return clean_text(og.get("content"))

    selectors = [
        ".product.media img[src]",
        ".gallery-placeholder img[src]",
        ".fotorama__stage__frame img[src]",
        "img[src]",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        src = node.get("src")
        if src:
            return urljoin(BASE_URL, clean_text(src))

    return ""


def extract_short_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("h1")
    if not h1:
        return ""
    title = clean_text(h1.get_text(" ", strip=True))
    title = re.sub(r"\s+", " ", title).strip()
    return title


def extract_description_text(soup: BeautifulSoup) -> str:
    possible_blocks: List[str] = []

    for selector in (
        '[data-content-type="description"]',
        ".product.attribute.description .value",
        ".product.attribute.description",
        "#description",
        ".product.info.detailed",
    ):
        for node in soup.select(selector):
            txt = clean_multiline_text(node.get_text("\n", strip=True))
            txt = re.sub(r"\bMnenja\b.*", "", txt, flags=re.IGNORECASE | re.DOTALL)
            txt = re.sub(r"\bNasveti\b.*", "", txt, flags=re.IGNORECASE | re.DOTALL)
            txt = clean_multiline_text(txt)
            if txt and len(txt) > 40:
                possible_blocks.append(txt)

    if possible_blocks:
        return safe_truncate(max(possible_blocks, key=len))

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return safe_truncate(clean_text(meta_desc.get("content")))

    return ""


def extract_sku(soup: BeautifulSoup, product_url: str) -> str:
    page_text = soup.get_text("\n", strip=True)

    match = re.search(r"Šifra izdelka\s*:\s*([0-9A-Za-z\-_/]+)", page_text, flags=re.IGNORECASE)
    if match:
        return clean_text(match.group(1))

    match = re.search(r"Št\.\s*art\.\s*:\s*([0-9A-Za-z\-_/]+)", page_text, flags=re.IGNORECASE)
    if match:
        return clean_text(match.group(1))

    url_match = re.search(r"-([0-9]{4,})/?$", product_url.strip("/"))
    if url_match:
        return url_match.group(1)

    return ""


def extract_manufacturer(soup: BeautifulSoup) -> str:
    page_text = soup.get_text("\n", strip=True).replace("\xa0", " ")

    match = re.search(r"Proizvajalec(?:/Uvoznik)?\s*:\s*([^\n\r]+)", page_text, flags=re.IGNORECASE)
    if match:
        return clean_text(match.group(1))[:250]

    match = re.search(r"Blagovna znamka\s+([^\n\r]+)", page_text, flags=re.IGNORECASE)
    if match:
        return clean_text(match.group(1))[:250]

    return ""


def _extract_stock_section_lines(text: str) -> List[str]:
    lines = [clean_text(x) for x in text.replace("\xa0", " ").splitlines()]
    lines = [x for x in lines if x]

    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        if line.lower() == "zaloga v trgovskih centrih":
            start_idx = i + 1
            break

    if start_idx is None:
        return []

    for j in range(start_idx, len(lines)):
        low = lines[j].lower()
        if low.startswith("brezplačna pomoč pri nakupu") or low.startswith("izračun mesečnega obroka") or low.startswith("povpraševanje"):
            end_idx = j
            break

    if end_idx is None:
        end_idx = len(lines)

    return lines[start_idx:end_idx]


def _parse_store_lines_to_centers(lines: List[str]) -> Dict[str, str]:
    centers: Dict[str, str] = {}
    pending_status: Optional[str] = None

    for line in lines:
        low = line.lower().strip()

        if low in {"na zalogi", "ni zaloge", "zadnji kosi"}:
            pending_status = line
            continue

        if low.startswith("merkur "):
            center_name = re.sub(r"\(\+386.*$", "", line).strip()
            center_name = clean_text(center_name)
            if center_name:
                if pending_status:
                    centers[center_name] = pending_status
                    pending_status = None
                elif center_name not in centers:
                    centers[center_name] = ""

    return centers


def extract_stock_data(soup: BeautifulSoup) -> Tuple[str, str, Dict[str, str]]:
    text = soup.get_text("\n", strip=True).replace("\xa0", " ")
    centers: Dict[str, str] = {}

    # 1) najprej preberi blok "Zaloga v trgovskih centrih"
    section_lines = _extract_stock_section_lines(text)
    if section_lines:
        centers = _parse_store_lines_to_centers(section_lines)

    if centers:
        positive_values = {"na zalogi", "zadnji kosi"}
        has_positive = any((v or "").lower() in positive_values for v in centers.values())
        dobava = "DA" if has_positive else "NE"
        return dobava, json.dumps(centers, ensure_ascii=False), centers

    # 2) fallback: stari numerični vzorec
    matches = re.findall(r"(MERKUR[^\n\r]+?)\s+(\d+)\s+kos", text, flags=re.IGNORECASE)
    for name, qty in matches:
        center_name = clean_text(re.sub(r"\(\+386.*$", "", name).strip())
        qty_text = clean_text(qty)
        if center_name and qty_text:
            centers[center_name] = qty_text

    if centers:
        has_positive = any(int(v) > 0 for v in centers.values() if str(v).isdigit())
        dobava = "DA" if has_positive else "NE"
        return dobava, json.dumps(centers, ensure_ascii=False), centers

    # 3) fallback: splošni status na vrhu produkta
    lower = text.lower()
    if "ni na zalogi" in lower or "ni zaloge" in lower:
        return "NE", "", {}
    if "na zalogi" in lower:
        return "DA", "", {}

    match = re.search(r"(\d+\s*[-–]\s*\d+\s*delovnih\s*dni)", text, flags=re.IGNORECASE)
    if match:
        return clean_text(match.group(1)), "", {}

    return "", "", {}


def extract_price_text_candidates(soup: BeautifulSoup) -> str:
    parts: List[str] = []

    selectors = [
        ".product-info-price",
        ".price-box",
        ".price-wrapper",
        ".product-info-main",
    ]
    for selector in selectors:
        for node in soup.select(selector):
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                parts.append(text)

    return " | ".join(dict.fromkeys(parts))


def extract_prices_and_em(soup: BeautifulSoup, title: str, description: str) -> Tuple[str, str, str, str, str]:
    text = extract_price_text_candidates(soup)

    price_per_unit, unit = extract_price_per_unit(text)
    all_prices = extract_all_prices(text)

    regular_price = ""
    sale_price = ""

    if price_per_unit:
        regular_price = price_per_unit
        unit = normalize_em(unit)
    else:
        regular_price = extract_first_price(text)
        unit = ""

    if not unit:
        guessed = normalize_em(guess_em_from_text(title))
        unit = guessed if guessed else "kos"

    if unit == "kos":
        title_lower = f"{title} {description}".lower()
        if re.search(r"\b(m2|m²)\b", title_lower):
            if re.search(r"€\s*/\s*(m2|m²)", text, flags=re.IGNORECASE):
                unit = "m2"

    return (
        regular_price,
        sale_price,
        convert_price_to_without_vat(regular_price, DDV_RATE),
        convert_price_to_without_vat(sale_price, DDV_RATE),
        unit or "kos",
    )


def build_record(
    product_url: str,
    category_slug: str,
    date_str: str,
    soup: BeautifulSoup,
    next_zap: int,
) -> Dict[str, str]:
    title = extract_short_title(soup)
    description = extract_description_text(soup)
    sku = extract_sku(soup, product_url)
    ean = extract_ean_raw(soup.get_text("\n", strip=True))
    manufacturer = extract_manufacturer(soup)
    image_url = extract_main_image(soup)
    dobava, stock_json, centers = extract_stock_data(soup)
    price_w_vat, sale_w_vat, price_wo_vat, sale_wo_vat, em = extract_prices_and_em(soup, title, description)

    row = get_base_record()
    row.update(
        {
            "Skupina": category_slug,
            "Zap": next_zap,
            "Oznaka / naziv": sku,
            "EAN": ean,
            "Opis": title,
            "Opis izdelka": description,
            "Varianta": "",
            "EM": normalize_em(em),
            "Valuta": "EUR",
            "DDV": "22",
            "Proizvajalec": manufacturer,
            "Veljavnost od": date_str,
            "Dobava": dobava,
            "Zaloga po centrih": stock_json,
            "Cena / EM (z DDV)": round_price_2dec(price_w_vat) if price_w_vat else "",
            "Akcijska cena / EM (z DDV)": round_price_2dec(sale_w_vat) if sale_w_vat else "",
            "Cena / EM (brez DDV)": round_price_2dec(price_wo_vat) if price_wo_vat else "",
            "Akcijska cena / EM (brez DDV)": round_price_2dec(sale_wo_vat) if sale_wo_vat else "",
            "URL": product_url,
            "SLIKA URL": image_url,
        }
    )

    for center_name, value in centers.items():
        row[f"Zaloga - {center_name}"] = value

    return row


def get_product_details(
    session,
    user_agent: str,
    product_url: str,
    category_slug: str,
    date_str: str,
    next_zap: int,
    logger,
) -> Optional[Dict[str, str]]:
    logger.log(f"    - Detajli: {product_url}")

    html = get_page_content(
        session=session,
        url=product_url,
        base_url=BASE_URL,
        user_agent=user_agent,
        referer=BASE_URL,
        timeout=25,
        retries=3,
        sleep_min=1.0,
        sleep_max=2.8,
        logger=logger,
    )
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    title = extract_short_title(soup)
    if not title:
        return None

    return build_record(product_url, category_slug, date_str, soup, next_zap)


def scrape_merkur() -> Tuple[str, str, str]:
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
        date_str = datetime.now().strftime("%Y-%m-%d")

        session = build_session()
        user_agent = random.choice(DEFAULT_USER_AGENTS)
        warmup_session(session, BASE_URL, user_agent)

        batch_rows: List[Dict[str, str]] = []
        processed_products = 0

        for group_name, category_urls in MERKUR_CATEGORIES.items():
            logger.log("")
            logger.log(f"--- {group_name} ---")

            for category_url in category_urls:
                category_slug = category_url.rstrip("/").split("/")[-1]
                logger.log("")
                logger.log(f"  Kategorija: {category_slug}")

                try:
                    product_links = get_product_links_from_category(session, user_agent, category_url, logger)
                except Exception as exc:
                    logger.log(f"  Napaka pri kategoriji {category_url}: {exc}")
                    continue

                if not product_links:
                    logger.log("  Ni najdenih produktnih linkov.")
                    continue

                for product_url in product_links:
                    try:
                        row = get_product_details(
                            session=session,
                            user_agent=user_agent,
                            product_url=product_url,
                            category_slug=category_slug,
                            date_str=date_str,
                            next_zap=next_zap,
                            logger=logger,
                        )
                        if row:
                            batch_rows.append(row)
                            next_zap += 1
                            processed_products += 1

                        if len(batch_rows) >= BATCH_SIZE:
                            save_data_batch_json_only(
                                new_data=batch_rows,
                                json_path=json_path,
                                excel_path=excel_path,
                                logger=logger,
                                use_variant=False,
                            )
                            batch_rows = []

                        batch_pause(
                            processed_count=processed_products,
                            every_n=40,
                            pause_min=8.0,
                            pause_max=18.0,
                            logger=logger,
                        )

                    except Exception as exc:
                        logger.log(f"    Napaka pri izdelku {product_url}: {exc}")
                        continue

        if batch_rows:
            save_data_batch_json_only(
                new_data=batch_rows,
                json_path=json_path,
                excel_path=excel_path,
                logger=logger,
                use_variant=False,
            )

        final_data = load_existing_data(json_path, excel_path)
        columns = build_excel_columns(merge_extra_columns_from_data(final_data))
        write_excel_from_json(json_path, excel_path, columns, logger=logger)

        logger.log(f"Konec. Skupno zapisov: {len(final_data)}")
        return json_path, excel_path, log_path

    finally:
        logger.close()


if __name__ == "__main__":
    scrape_merkur()
