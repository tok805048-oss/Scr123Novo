import random
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from categories.tehnoles_categories import TEHNOLES_CATEGORIES
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


SHOP_NAME = "Tehnoles"
BASE_URL = "https://www.tehnoles.si"
DDV_RATE = 0.22
BATCH_SIZE = 30
MAX_PAGES = 250


def build_page_url(category_url: str, page: int) -> str:
    sep = "&" if "?" in category_url else "?"
    return f"{category_url}{sep}pagenum={page}"


def extract_product_links_from_category_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []

    for item in soup.select("li.wrapper_prods.category"):
        a_tag = item.select_one(".name a[href]") or item.select_one("a[href]")
        if not a_tag:
            continue

        href = clean_text(a_tag.get("href"))
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)
        if full_url.startswith(BASE_URL) and full_url.lower().endswith(".aspx"):
            links.append(full_url)

    return list(dict.fromkeys(links))


def get_product_links_from_category(session, user_agent: str, category_url: str, logger) -> List[str]:
    all_links: List[str] = []
    seen_first_url: set[str] = set()

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
        products = soup.select("li.wrapper_prods.category")
        if not products:
            break

        first_a = products[0].select_one(".name a[href]") or products[0].select_one("a[href]")
        first_href = urljoin(BASE_URL, first_a.get("href")) if first_a and first_a.get("href") else ""
        if first_href and first_href in seen_first_url:
            logger.log("  Stran se ponavlja. Konec kategorije.")
            break
        if first_href:
            seen_first_url.add(first_href)

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

        if not soup.select_one("a.PagerPrevNextLink"):
            break

    return all_links


def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("h1.productInfo") or soup.select_one("h1")
    return clean_text(h1.get_text(" ", strip=True)) if h1 else ""


def extract_ident_and_url_sku(soup: BeautifulSoup, product_url: str) -> Tuple[str, str]:
    page_text = soup.get_text("\n", strip=True).replace("\xa0", " ")

    ident = ""
    url_sku = ""

    m_ident = re.search(r"Ident\s*:\s*([0-9A-Za-z\-_./]+)", page_text, flags=re.IGNORECASE)
    if m_ident:
        ident = clean_text(m_ident.group(1))

    m_url = re.search(r"-p-(\d+)\.aspx", product_url, flags=re.IGNORECASE)
    if m_url:
        url_sku = clean_text(m_url.group(1))

    return ident, url_sku


def extract_manufacturer(soup: BeautifulSoup) -> str:
    page_text = soup.get_text("\n", strip=True).replace("\xa0", " ")
    m = re.search(r"Proizvajalec\s*:\s*([^\n\r]+)", page_text, flags=re.IGNORECASE)
    if m:
        return clean_text(m.group(1))[:250]
    return ""


def extract_em(soup: BeautifulSoup, title: str) -> str:
    page_text = soup.get_text("\n", strip=True).replace("\xa0", " ")

    m = re.search(r"Enota mere\s*:\s*([A-Za-z0-9²³/]+)", page_text, flags=re.IGNORECASE)
    if m:
        return normalize_em(m.group(1))

    title_l = title.lower().replace(" ", "")
    if "m2/pkt" in title_l or "m2kos" in title_l or "m2/kos" in title_l:
        return "m2"
    if "m3" in title_l:
        return "m3"
    if "kg" in title_l:
        return "kg"
    if "l" in title_l:
        return "L"

    return "kos"


def extract_price_and_sale(soup: BeautifulSoup) -> Tuple[str, str]:
    page_text = soup.get_text("\n", strip=True).replace("\xa0", " ")

    sale_price = ""
    regular_price = ""

    m_sale = re.search(r"Vaša cena z DDV\s*:\s*([0-9.,]+)", page_text, flags=re.IGNORECASE)
    if m_sale:
        sale_price = round_price_2dec(m_sale.group(1))

    m_regular = re.search(r"Najnižja cena zadnjih 30 dni\s*:\s*([0-9.,]+)", page_text, flags=re.IGNORECASE)
    if m_regular:
        regular_price = round_price_2dec(m_regular.group(1))

    # Če akcijske ni, uporabljamo glavno kot redno
    if sale_price and not regular_price:
        regular_price = sale_price
        sale_price = ""

    return regular_price, sale_price


def extract_delivery(soup: BeautifulSoup) -> str:
    page_text = soup.get_text(" ", strip=True).lower().replace("\xa0", " ")

    if "na zalogi" in page_text:
        return "DA"
    if "ni na zalogi" in page_text or "ni zaloge" in page_text:
        return "NE"

    m = re.search(r"(dobavni rok|dobava)\s*[:\-]?\s*([0-9]+\s*[-–]\s*[0-9]+\s*\w+)", page_text, flags=re.IGNORECASE)
    if m:
        return clean_text(m.group(2))

    return ""


def extract_long_description(soup: BeautifulSoup) -> str:
    lines = [
        re.sub(r"\s+", " ", ln).strip()
        for ln in soup.get_text("\n", strip=True).replace("\xa0", " ").splitlines()
    ]
    lines = [ln for ln in lines if ln]

    start_idx = None
    for i, line in enumerate(lines):
        if line.lower() == "opis":
            start_idx = i
            break

    if start_idx is not None:
        stop_headers = {
            "komentarji in ocene",
            "priporoči prijatelju",
            "povpraševanje o izdelku",
            "potrebujete dodatno pomoč?",
        }
        out: List[str] = []
        for line in lines[start_idx + 1:]:
            low = line.lower().strip()
            if low in stop_headers or any(low.startswith(h) for h in stop_headers):
                break
            out.append(line)

        desc = clean_multiline_text("\n".join(out)).strip()
        if desc:
            return safe_truncate(desc)

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return safe_truncate(clean_text(meta_desc.get("content")))

    return ""


def extract_image_url(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return clean_text(og.get("content"))

    img = soup.select_one("img[src]")
    if img and img.get("src"):
        return urljoin(BASE_URL, clean_text(img.get("src")))

    return ""


def build_record(
    product_url: str,
    category_name: str,
    today_str: str,
    soup: BeautifulSoup,
    next_zap: int,
) -> Dict[str, Any]:
    title = extract_title(soup)
    ident, url_sku = extract_ident_and_url_sku(soup, product_url)
    description = extract_long_description(soup)
    ean = extract_ean_raw(soup)
    manufacturer = extract_manufacturer(soup)
    em = extract_em(soup, title)
    regular_price, sale_price = extract_price_and_sale(soup)
    image_url = extract_image_url(soup)
    dobava = extract_delivery(soup)

    # Pri Tehnoles uporabi Ident, če obstaja; URL SKU naj bo fallback
    sku = ident or url_sku

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
            "Dobava": dobava,
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


def scrape_tehnoles() -> Tuple[str, str, str]:
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

        for group_name, category_urls in TEHNOLES_CATEGORIES.items():
            logger.log("")
            logger.log(f"--- {group_name} ---")

            for category_url in category_urls:
                category_name = category_url.split("/")[-1].split("-c-")[0].strip() or group_name
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
    scrape_tehnoles()
