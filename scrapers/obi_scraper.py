import json
import random
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from categories.obi_categories import OBI_CATEGORIES
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


SHOP_NAME = "OBI"
BASE_URL = "https://www.obi.si"
DDV_RATE = 0.22
BATCH_SIZE = 30
MAX_PAGES = 250

OBI_STORES_ORDER = [
    "OBI Spletna trgovina",
    "OBI Celje",
    "OBI Koper",
    "OBI Kranj",
    "OBI Ljubljana",
    "OBI Maribor",
    "OBI Murska Sobota",
    "OBI Nova Gorica",
    "OBI Ptuj",
]


def add_or_replace_query(url: str, params: Dict[str, str]) -> str:
    parts = urlparse(url)
    q = dict(parse_qsl(parts.query))
    q.update(params)
    new_query = urlencode(q)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def build_page_url(category_url: str, page: int) -> str:
    return add_or_replace_query(category_url, {"p": str(page)})


def normalize_url(href: str) -> str:
    return urljoin(BASE_URL, href)


def extract_product_links_from_category_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []

    container = soup.find("div", class_="list-items list-category-products")
    if not container:
        container = soup.find("div", class_="list-items")
    if not container:
        return links

    items = container.find_all("div", class_="item")
    for item in items:
        a_tag = item.find("a", href=True)
        if not a_tag:
            continue

        href = clean_text(a_tag.get("href"))
        if not href or "/p/" not in href:
            continue

        full_url = normalize_url(href)
        if full_url.startswith(BASE_URL):
            links.append(full_url)

    return list(dict.fromkeys(links))


def get_product_links_from_category(session, user_agent: str, category_url: str, logger) -> List[str]:
    all_links: List[str] = []
    seen_first_title: set[str] = set()

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
        container = soup.find("div", class_="list-items list-category-products")
        if not container:
            container = soup.find("div", class_="list-items")
        if not container:
            break

        items = container.find_all("div", class_="item")
        if not items:
            break

        first_title_el = items[0].find("h4")
        first_title = clean_text(first_title_el.get_text(" ", strip=True)) if first_title_el else ""
        if first_title and first_title in seen_first_title:
            logger.log("  Stran se ponavlja. Konec kategorije.")
            break
        if first_title:
            seen_first_title.add(first_title)

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

    return all_links


def extract_product_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("div.product-basics-info.part-1 h1") or soup.select_one("h1")
    if h1:
        return clean_text(h1.get_text(" ", strip=True))
    title = soup.select_one("title")
    return clean_text(title.get_text(" ", strip=True)) if title else ""


def extract_product_id_number(soup: BeautifulSoup, product_url: str) -> str:
    txt = soup.get_text(" ", strip=True).replace("\xa0", " ")

    match = re.search(r"Št\.?\s*art\.?\s*:\s*([0-9]+)", txt, flags=re.IGNORECASE)
    if match:
        return clean_text(match.group(1))

    url_match = re.search(r"-([0-9]{6,})/?$", product_url.strip("/"))
    if url_match:
        return url_match.group(1)

    return ""


def extract_product_long_description(soup: BeautifulSoup) -> str:
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]

    idx = None
    for i, line in enumerate(lines):
        if line.lower().strip(": ") == "opis":
            idx = i
            break

    if idx is None:
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            return safe_truncate(clean_text(meta_desc.get("content")))
        return ""

    stop_headers = {
        "podatki proizvajalca",
        "tehnične lastnosti",
        "ocene",
        "dokumenti",
        "nazadnje ogledani izdelki",
        "prijava na spletne novice",
        "4 razlogi za nakup brez skrbi",
        "preberi več ...",
    }

    out: List[str] = []
    for line in lines[idx + 1:]:
        low = line.lower().strip()
        if low in stop_headers or any(low.startswith(h) for h in stop_headers):
            break
        out.append(line)

    desc = clean_multiline_text("\n".join(out)).strip()
    return safe_truncate(desc)


def extract_manufacturer(soup: BeautifulSoup) -> str:
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]

    idx = None
    for i, line in enumerate(lines):
        if line.lower().strip() == "podatki proizvajalca":
            idx = i
            break

    if idx is None:
        return ""

    collected: List[str] = []
    for line in lines[idx + 1:]:
        low = line.lower().strip()
        if low.startswith("tehnične lastnosti") or low.startswith("ocene") or low.startswith("dokumenti"):
            break
        collected.append(line)

    if not collected:
        return ""

    # prva vrstica v sekciji proizvajalca je običajno ime proizvajalca
    return clean_text(collected[0])[:250]


def extract_image_url(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return clean_text(og.get("content"))

    img = soup.select_one("img[src]")
    if img and img.get("src"):
        return normalize_url(img.get("src"))

    return ""


def extract_price_text(soup: BeautifulSoup) -> str:
    parts: List[str] = []

    selectors = [
        ".price",
        ".price-box",
        ".product-price",
        ".product-basics-info",
        ".product-main",
    ]
    for selector in selectors:
        for node in soup.select(selector):
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                parts.append(text)

    return " | ".join(dict.fromkeys(parts))


def extract_prices_and_em(soup: BeautifulSoup, title: str, description: str) -> Tuple[str, str, str, str, str]:
    price_text = extract_price_text(soup)

    all_prices = extract_all_prices(price_text)
    regular_price = ""
    sale_price = ""

    if all_prices:
        regular_price = all_prices[0]
    else:
        first_price = extract_first_price(price_text)
        if first_price:
            regular_price = first_price

    # OBI praviloma prikazuje glavno ceno izdelka, ne cene na EM
    unit = normalize_em(guess_em_from_text(title))
    if not unit:
        unit = "kos"

    # če je v naslovu jasno izražen kg/l/m2 in je to tudi prodajna enota, jo lahko uporabimo
    title_desc = f"{title} {description}".lower()
    if re.search(r"\bkg\b", title_desc):
        unit = "kg"
    elif re.search(r"\b(m2|m²)\b", title_desc):
        unit = "m2"
    elif re.search(r"\bl\b", title_desc):
        unit = "L"
    elif not unit:
        unit = "kos"

    return (
        round_price_2dec(regular_price) if regular_price else "",
        round_price_2dec(sale_price) if sale_price else "",
        convert_price_to_without_vat(regular_price, DDV_RATE) if regular_price else "",
        convert_price_to_without_vat(sale_price, DDV_RATE) if sale_price else "",
        unit or "kos",
    )


def extract_store_stock(soup: BeautifulSoup) -> Dict[str, int]:
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")

    stock: Dict[str, int] = {}

    matches = re.findall(r"(OBI[^\n\r]+?)\s+(\d+)\s+kosov", txt, flags=re.IGNORECASE)
    for name, qty in matches:
        store_name = clean_text(name)
        try:
            stock[store_name] = int(qty)
        except Exception:
            continue

    return stock


def extract_delivery_and_stock(soup: BeautifulSoup) -> Tuple[str, str, Dict[str, Any]]:
    stock = extract_store_stock(soup)
    page_text = soup.get_text(" ", strip=True).replace("\xa0", " ")

    if stock:
        dobava = "DA" if any(qty > 0 for qty in stock.values()) else "NE"
        return dobava, json.dumps(stock, ensure_ascii=False), stock

    if "Premalo zalog" in page_text or "Ni na voljo" in page_text:
        return "NE", "", {}

    if "Na zalogi" in page_text:
        return "DA", "", {}

    return "", "", {}


def build_record(
    product_url: str,
    category_name: str,
    today_str: str,
    soup: BeautifulSoup,
    next_zap: int,
) -> Dict[str, Any]:
    title = extract_product_title(soup)
    long_description = extract_product_long_description(soup)
    product_id = extract_product_id_number(soup, product_url)
    ean = extract_ean_raw(soup.get_text(" ", strip=True))
    manufacturer = extract_manufacturer(soup)
    image_url = extract_image_url(soup)
    dobava, stock_json, stock_dict = extract_delivery_and_stock(soup)
    price_w_vat, sale_w_vat, price_wo_vat, sale_wo_vat, em = extract_prices_and_em(
        soup, title, long_description
    )

    row = get_base_record()
    row.update(
        {
            "Skupina": category_name,
            "Zap": next_zap,
            "Oznaka / naziv": product_id,
            "EAN": ean,
            "Opis": title,
            "Opis izdelka": long_description,
            "Varianta": "",
            "EM": normalize_em(em) if em else "kos",
            "Valuta": "EUR",
            "DDV": "22",
            "Proizvajalec": manufacturer,
            "Veljavnost od": today_str,
            "Dobava": dobava,
            "Zaloga po centrih": stock_json,
            "Cena / EM (z DDV)": price_w_vat,
            "Akcijska cena / EM (z DDV)": sale_w_vat,
            "Cena / EM (brez DDV)": price_wo_vat,
            "Akcijska cena / EM (brez DDV)": sale_wo_vat,
            "URL": product_url,
            "SLIKA URL": image_url,
        }
    )

    for store in OBI_STORES_ORDER:
        row[f"Zaloga - {store}"] = stock_dict.get(store, 0)

    return row


def get_product_details(
    session,
    user_agent: str,
    product_url: str,
    category_name: str,
    today_str: str,
    next_zap: int,
    logger,
    referer: str,
) -> Optional[Dict[str, Any]]:
    logger.log(f"    - Detajli: {product_url}")

    html = get_page_content(
        session=session,
        url=product_url,
        base_url=BASE_URL,
        user_agent=user_agent,
        referer=referer,
        timeout=25,
        retries=3,
        sleep_min=1.0,
        sleep_max=2.8,
        logger=logger,
    )
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    title = extract_product_title(soup)
    if not title:
        return None

    return build_record(product_url, category_name, today_str, soup, next_zap)


def scrape_obi() -> Tuple[str, str, str]:
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

        batch_rows: List[Dict[str, Any]] = []
        processed_products = 0

        for group_name, category_urls in OBI_CATEGORIES.items():
            logger.log("")
            logger.log(f"--- {group_name} ---")

            for category_url in category_urls:
                category_name = category_url.rstrip("/").split("/")[-1]
                logger.log("")
                logger.log(f"  Kategorija: {category_name}")

                try:
                    product_links = get_product_links_from_category(
                        session=session,
                        user_agent=user_agent,
                        category_url=category_url,
                        logger=logger,
                    )
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
                            category_name=group_name,
                            today_str=today_str,
                            next_zap=next_zap,
                            logger=logger,
                            referer=category_url,
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
    scrape_obi()
