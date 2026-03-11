import random
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from categories.zagozen_categories import ZAGOZEN_CATEGORIES
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
from common.text_utils import (
    clean_multiline_text,
    clean_text,
    extract_ean_raw,
    safe_truncate,
)
from common.unit_utils import normalize_em


SHOP_NAME = "Zagozen"
BASE_URL = "https://eshop-zagozen.si"
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
    # Zagožen kaže paginacijo kot ?page=
    return add_or_replace_query(category_url, {"page": str(page)})


def extract_product_links_from_category_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []

    selectors = [
        "li.product a[href]",
        ".products li a[href]",
        ".product a[href]",
        "a.woocommerce-LoopProduct-link[href]",
    ]

    for selector in selectors:
        for a_tag in soup.select(selector):
            href = clean_text(a_tag.get("href"))
            if not href:
                continue

            full_url = urljoin(BASE_URL, href)
            if not full_url.startswith(BASE_URL):
                continue

            low = full_url.lower()
            if "/product-category/" in low or "/kategorija" in low:
                continue

            links.append(full_url)

    return list(dict.fromkeys(links))


def get_product_links_from_category(session, user_agent: str, category_url: str, logger) -> List[str]:
    all_links: List[str] = []
    seen_first: set[str] = set()

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

        page_links = extract_product_links_from_category_html(html)
        if not page_links:
            break

        first_url = page_links[0]
        if first_url in seen_first:
            logger.log("  Stran se ponavlja. Konec kategorije.")
            break
        seen_first.add(first_url)

        new_count = 0
        for link in page_links:
            if link not in all_links:
                all_links.append(link)
                new_count += 1

        if new_count == 0:
            break

        soup = BeautifulSoup(html, "html.parser")
        if not soup.select_one(".pagination, .woocommerce-pagination, a.next, a.next.page-numbers"):
            if page >= 2:
                break

    return all_links


def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("h1.product_title") or soup.select_one("h1")
    return clean_text(h1.get_text(" ", strip=True)) if h1 else ""


def extract_sku(soup: BeautifulSoup, product_url: str) -> str:
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")

    m = re.search(r"Šifra artikla\s*:\s*([0-9A-Za-z\-_./]+)", txt, flags=re.IGNORECASE)
    if m:
        return clean_text(m.group(1))

    sku_node = soup.select_one(".sku")
    if sku_node and sku_node.get_text(strip=True):
        return clean_text(sku_node.get_text(" ", strip=True))

    slug = urlparse(product_url).path.strip("/").split("/")[-1]
    return clean_text(slug)[:120]


def extract_image_url(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return urljoin(BASE_URL, clean_text(og.get("content")))

    img = (
        soup.select_one(".woocommerce-product-gallery__image img[src]")
        or soup.select_one("img.wp-post-image[src]")
        or soup.select_one("img[src]")
    )
    if img and img.get("src"):
        return urljoin(BASE_URL, clean_text(img.get("src")))

    return ""


def extract_description(soup: BeautifulSoup) -> str:
    for selector in (
        ".woocommerce-Tabs-panel--description",
        "#tab-description",
        ".product .description",
        ".entry-content",
    ):
        node = soup.select_one(selector)
        if node:
            txt = clean_multiline_text(node.get_text("\n", strip=True))
            if txt and len(txt) > 20:
                return safe_truncate(txt)

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return safe_truncate(clean_text(meta_desc.get("content")))

    return ""


def extract_prices_and_em(soup: BeautifulSoup) -> Tuple[str, str, str]:
    page_text = soup.get_text("\n", strip=True).replace("\xa0", " ")

    regular_price = ""
    sale_price = ""
    em = "kos"

    # akcijska cena
    m_sale = re.search(r"Akcijska cena\s*([0-9.,]+)", page_text, flags=re.IGNORECASE)
    if m_sale:
        sale_price = round_price_2dec(m_sale.group(1))

    # redna cena
    m_price = re.search(r"Cena\s*:\s*([0-9.,]+)", page_text, flags=re.IGNORECASE)
    if m_price:
        regular_price = round_price_2dec(m_price.group(1))

    # če je na strani samo ena cena v woocommerce blocku
    if not regular_price:
        amt = soup.select_one(".woocommerce-Price-amount")
        if amt and amt.get_text(strip=True):
            regular_price = round_price_2dec(amt.get_text(" ", strip=True))

    # EM
    m_em = re.search(r"Cena je (?:na|za)\s+([A-Za-z0-9²³/]+)", page_text, flags=re.IGNORECASE)
    if m_em:
        em = normalize_em(m_em.group(1))

    return regular_price, sale_price, em or "kos"


def extract_delivery(soup: BeautifulSoup) -> str:
    txt = soup.get_text(" ", strip=True).replace("\xa0", " ")

    m = re.search(r"Dobava\s*:\s*([0-9]+\s*[-–]\s*[0-9]+\s*delovnih\s*dni)", txt, flags=re.IGNORECASE)
    if m:
        return clean_text(m.group(1))

    low = txt.lower()
    if "na zalogi" in low or "in stock" in low:
        return "DA"
    if "ni na zalogi" in low or "out of stock" in low:
        return "NE"

    return ""


def extract_manufacturer(soup: BeautifulSoup) -> str:
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    m = re.search(r"Proizvajalec\s*:\s*([^\n\r]+)", txt, flags=re.IGNORECASE)
    if m:
        return clean_text(m.group(1))[:250]
    return ""


def build_record(
    product_url: str,
    category_name: str,
    today_str: str,
    soup: BeautifulSoup,
    next_zap: int,
) -> Dict[str, Any]:
    title = extract_title(soup)
    sku = extract_sku(soup, product_url)
    description = extract_description(soup)
    ean = extract_ean_raw(soup.get_text(" ", strip=True))
    manufacturer = extract_manufacturer(soup)
    regular_price, sale_price, em = extract_prices_and_em(soup)
    delivery = extract_delivery(soup)
    image_url = extract_image_url(soup)

    row = get_base_record()
    row.update(
        {
            "Skupina": category_name,
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
            "Veljavnost od": today_str,
            "Dobava": delivery,
            "Cena / EM (z DDV)": regular_price,
            "Akcijska cena / EM (z DDV)": sale_price,
            "Cena / EM (brez DDV)": convert_price_to_without_vat(regular_price, DDV_RATE) if regular_price else "",
            "Akcijska cena / EM (brez DDV)": convert_price_to_without_vat(sale_price, DDV_RATE) if sale_price else "",
            "URL": product_url,
            "SLIKA URL": image_url,
        }
    )

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
    title = extract_title(soup)
    if not title:
        return None

    return build_record(product_url, category_name, today_str, soup, next_zap)


def scrape_zagozen() -> Tuple[str, str, str]:
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

        for group_name, category_urls in ZAGOZEN_CATEGORIES.items():
            logger.log("")
            logger.log(f"--- {group_name} ---")

            for category_url in category_urls:
                category_name = category_url.rstrip("/").split("/")[-1] or group_name
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
                            category_name=category_name,
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
    scrape_zagozen()
