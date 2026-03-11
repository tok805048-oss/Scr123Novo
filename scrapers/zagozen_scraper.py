import random
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

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


def build_page_urls(category_url: str, page: int) -> List[str]:
    """
    Zagožen se bolje obnaša z WooCommerce-style /page/2/ URL-ji.
    Kot fallback dodamo še ?paged=.
    """
    urls: List[str] = []

    if page == 1:
        urls.append(category_url.rstrip("/"))
    else:
        urls.append(category_url.rstrip("/") + f"/page/{page}/")
        if "?" in category_url:
            urls.append(category_url + f"&paged={page}")
        else:
            urls.append(category_url.rstrip("/") + f"?paged={page}")

    return urls


def _is_product_url(url: str) -> bool:
    if not url:
        return False
    if not url.startswith(BASE_URL):
        return False

    low = url.lower()
    blocked = (
        "/tag/",
        "/author/",
        "/category/",
        "/produkt-kategorija/",
        "/cart/",
        "/checkout/",
        "/my-account/",
        "mailto:",
        "javascript:",
        "#",
    )
    if any(x in low for x in blocked):
        return False

    path = urlparse(url).path.strip("/")
    if not path:
        return False

    # produktni URL-ji so praviloma konkretni slugi, ne vrhnje kategorije
    top_level = {
        "vodovod",
        "kanalizacija",
        "energetika",
        "aktualno",
    }
    if path in top_level:
        return False

    return True


def extract_product_links_from_category_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []

    selectors = [
        # stari delujoči woo selektorji
        "li.product a[href]",
        "a.woocommerce-LoopProduct-link[href]",
        # dejanska struktura Zagožen kategorij
        ".product-grid a[href]",
        ".products a[href]",
        "article a[href]",
        # fallback: vsi linki v glavnem vsebinskem delu
        "main a[href]",
    ]

    for selector in selectors:
        for a_tag in soup.select(selector):
            href = clean_text(a_tag.get("href"))
            if not href:
                continue

            full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
            if not _is_product_url(full_url):
                continue

            text = clean_text(a_tag.get_text(" ", strip=True))
            # bonus filter: tipični produktni linki imajo vsaj nekaj vsebine ali slug
            if text or len(urlparse(full_url).path.strip("/").split("/")[-1]) > 6:
                links.append(full_url)

    return list(dict.fromkeys(links))


def get_product_links_from_category(session, user_agent: str, category_url: str, logger) -> List[str]:
    all_links: List[str] = []
    last_first: Optional[str] = None

    for page in range(1, MAX_PAGES + 1):
        candidate_urls = build_page_urls(category_url, page)
        html = None
        used_url = candidate_urls[0]

        for candidate in candidate_urls:
            logger.log(f"  Stran {page}: {candidate}")
            html = get_page_content(
                session=session,
                url=candidate,
                base_url=BASE_URL,
                user_agent=user_agent,
                referer=category_url,
                timeout=25,
                retries=3,
                sleep_min=1.0,
                sleep_max=2.8,
                logger=logger,
            )
            if html:
                used_url = candidate
                break

        if not html:
            break

        page_links = extract_product_links_from_category_html(html)
        if not page_links:
            break

        first_href = page_links[0]
        if page > 1 and last_first and first_href == last_first:
            logger.log("  Stran se ponavlja. Konec kategorije.")
            break
        last_first = first_href

        new_count = 0
        for link in page_links:
            if link not in all_links:
                all_links.append(link)
                new_count += 1

        if new_count == 0:
            break

        soup = BeautifulSoup(html, "html.parser")

        # če ni vidne pagination navigacije, po 1. strani vseeno še ne zaključimo,
        # ker imajo nekatere kategorije samo eno stran
        if page >= 2 and not soup.select_one(".pagination, nav.woocommerce-pagination, a.next, a.next.page-numbers"):
            break

    return list(dict.fromkeys(all_links))


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
        sku_text = clean_text(sku_node.get_text(" ", strip=True))
        if sku_text.lower() != "n/a":
            return sku_text

    slug = urlparse(product_url).path.strip("/").split("/")[-1]
    return clean_text(slug)[:120]


def extract_image_url(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        content = clean_text(og.get("content"))
        return content if content.startswith("http") else urljoin(BASE_URL, content)

    img = (
        soup.select_one(".woocommerce-product-gallery__image img[src]")
        or soup.select_one("img.wp-post-image[src]")
        or soup.select_one("img[src]")
    )
    if img and img.get("src"):
        src = clean_text(img.get("src"))
        return src if src.startswith("http") else urljoin(BASE_URL, src)

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

    m_sale = re.search(r"Akcijska cena\s*([0-9.,]+)", page_text, flags=re.IGNORECASE)
    if m_sale:
        sale_price = round_price_2dec(m_sale.group(1))

    m_price = re.search(r"Cena\s*:?\s*([0-9.,]+)", page_text, flags=re.IGNORECASE)
    if m_price:
        regular_price = round_price_2dec(m_price.group(1))

    if not regular_price:
        amounts = soup.select(".woocommerce-Price-amount")
        if amounts:
            vals = [round_price_2dec(x.get_text(" ", strip=True)) for x in amounts if x.get_text(" ", strip=True)]
            vals = [v for v in vals if v]
            if vals:
                regular_price = vals[0]
                if len(vals) >= 2:
                    sale_price = vals[1]

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
