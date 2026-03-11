import json
import random
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from categories.slovenijales_categories import SLOVENIJALES_CATEGORIES
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
    extract_price_per_unit,
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
from common.unit_utils import guess_em_from_text, normalize_em


SHOP_NAME = "Slovenijales"
BASE_URL = "https://trgovina.slovenijales.si"
DDV_RATE = 0.22
BATCH_SIZE = 30
MAX_PAGES = 250

STORE_FALLBACK = [
    "Slovenijales Maribor / Hoče",
    "Slovenijales Celje",
    "Hobby Ljubljana Črnuče",
    "Hobby Ljubljana Vižmarje",
    "Slovenijales Murska Sobota",
    "Slovenijales Nova Gorica",
    "Slovenijales Koper",
    "Slovenijales Kranj",
    "Slovenijales Novo mesto",
]


def add_or_replace_query(url: str, params: Dict[str, str]) -> str:
    parts = urlparse(url)
    q = dict(parse_qsl(parts.query))
    q.update(params)
    new_query = urlencode(q)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def build_page_url(category_url: str, page: int) -> str:
    return add_or_replace_query(category_url, {"page": str(page)})


def fetch_store_order(session, user_agent: str, logger) -> List[str]:
    url = f"{BASE_URL}/prodajni-centri"
    html = get_page_content(
        session=session,
        url=url,
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
        return STORE_FALLBACK

    soup = BeautifulSoup(html, "html.parser")
    candidates: List[str] = []

    for tag in soup.find_all(["h2", "h3", "h4", "strong"]):
        txt = clean_text(tag.get_text(" ", strip=True))
        if not txt:
            continue
        low = txt.lower()
        if low.startswith("slovenijales") or low.startswith("hobby") or "jelovica" in low:
            if len(txt) <= 80 and txt not in candidates:
                candidates.append(txt)

    return candidates or STORE_FALLBACK


def extract_product_links_from_category_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []

    products = soup.select('div.single-product.border-left[itemscope]')
    for p in products:
        a = p.select_one(".product-img a[href]")
        if not a:
            continue
        href = clean_text(a.get("href"))
        if not href:
            continue
        full = urljoin(BASE_URL, href)
        if full.startswith(BASE_URL):
            links.append(full)

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

        soup = BeautifulSoup(html, "html.parser")
        products = soup.select('div.single-product.border-left[itemscope]')
        if not products:
            break

        first_a = products[0].select_one(".product-img a[href]")
        first_url = urljoin(BASE_URL, first_a.get("href")) if first_a and first_a.get("href") else ""
        if first_url and first_url in seen_first:
            logger.log("  Stran se ponavlja. Konec kategorije.")
            break
        if first_url:
            seen_first.add(first_url)

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

        if not soup.select_one('ul.pagination a[aria-label="Naprej"]'):
            break

    return all_links


def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one('h1[itemprop="name"]') or soup.select_one("h1")
    return clean_text(h1.get_text(" ", strip=True)) if h1 else ""


def extract_sku(soup: BeautifulSoup) -> str:
    meta_sku = soup.select_one('meta[itemprop="sku"]')
    if meta_sku and meta_sku.get("content"):
        return clean_text(meta_sku.get("content"))

    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    m = re.search(r"Koda artikla\s*([0-9A-Za-z\-_./]+)", txt, flags=re.IGNORECASE)
    if m:
        return clean_text(m.group(1))

    return ""


def extract_image_url(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return clean_text(og.get("content"))

    img = (
        soup.select_one(".flexslider .slides img[src]")
        or soup.select_one('img[itemprop="image"][src]')
        or soup.select_one("img[src]")
    )
    if img and img.get("src"):
        return urljoin(BASE_URL, clean_text(img.get("src")))

    return ""


def extract_long_description(soup: BeautifulSoup) -> str:
    lines = [
        re.sub(r"\s+", " ", ln).strip()
        for ln in soup.get_text("\n", strip=True).replace("\xa0", " ").splitlines()
    ]
    lines = [ln for ln in lines if ln]

    start_idx = None
    for i, line in enumerate(lines):
        if line.lower() in {"opis izdelka", "opis"}:
            start_idx = i
            break

    if start_idx is not None:
        stop_headers = {
            "tehnične značilnosti",
            "sorodni artikli",
            "preveri razpoložljivost v poslovalnicah",
            "količina",
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


def extract_manufacturer(soup: BeautifulSoup) -> str:
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    m = re.search(r"Proizvajalec\s*[:\-]?\s*([^\n\r]+)", txt, flags=re.IGNORECASE)
    if m:
        return clean_text(m.group(1))[:250]
    return ""


def extract_prices_and_em(soup: BeautifulSoup, title: str, description: str) -> Tuple[str, str, str, str, str]:
    text = clean_text(soup.get_text(" ", strip=True).replace("\xa0", " "))

    price_per_unit, unit = extract_price_per_unit(text)
    all_prices = extract_all_prices(text)

    regular_price = ""
    sale_price = ""

    if price_per_unit:
        regular_price = price_per_unit
        unit = normalize_em(unit)
    elif all_prices:
        regular_price = all_prices[0]
    else:
        fp = extract_first_price(text)
        if fp:
            regular_price = fp

    if not unit:
        guessed = normalize_em(guess_em_from_text(title))
        unit = guessed if guessed else "kos"

    # Slovenijales pogosto pokaže: 44,10 € ... oz. 4,90 € / m2
    # Tu mora zmagati cena na EM, če obstaja.
    return (
        round_price_2dec(regular_price) if regular_price else "",
        round_price_2dec(sale_price) if sale_price else "",
        convert_price_to_without_vat(regular_price, DDV_RATE) if regular_price else "",
        convert_price_to_without_vat(sale_price, DDV_RATE) if sale_price else "",
        unit or "kos",
    )


def _normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("č", "c").replace("š", "s").replace("ž", "z")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _store_aliases(store_name: str) -> List[str]:
    aliases = [store_name]
    no_prefix = re.sub(r"^(Slovenijales|Hobby)\s+", "", store_name, flags=re.IGNORECASE).strip()
    if no_prefix and no_prefix not in aliases:
        aliases.append(no_prefix)

    for p in re.split(r"[/,]", store_name):
        p = p.strip()
        if p and p not in aliases:
            aliases.append(p)

    return aliases


def extract_store_stock_from_product_page(soup: BeautifulSoup, store_order: List[str]) -> Dict[str, int]:
    text = soup.get_text("\n", strip=True).replace("\xa0", " ")
    big_norm = _normalize_text(text)
    stock: Dict[str, int] = {}

    for store in store_order:
        aliases = _store_aliases(store)
        for alias in aliases:
            a_norm = _normalize_text(alias)
            if not a_norm or len(a_norm) < 4:
                continue
            idx = big_norm.find(a_norm)
            if idx < 0:
                continue

            window = big_norm[max(0, idx - 120): idx + 220]

            if "ni na zalogi" in window or "ni na voljo" in window:
                stock[store] = 0
                break

            if "na zalogi" in window or "na voljo" in window:
                mqty = re.search(r"(\d{1,4})\s*(kos|kom|m2|m3)?", window)
                if mqty:
                    try:
                        stock[store] = int(mqty.group(1))
                    except Exception:
                        stock[store] = 1
                else:
                    stock[store] = 1
                break

    return {k: v for k, v in stock.items() if isinstance(v, int) and v >= 0}


def extract_delivery_short(soup: BeautifulSoup) -> str:
    txt = soup.get_text(" ", strip=True).replace("\xa0", " ").lower()
    if "na zalogi" in txt:
        return "DA"
    if "ni na zalogi" in txt or "ni zaloge" in txt:
        return "NE"
    return ""


def build_record(
    product_url: str,
    category_name: str,
    today_str: str,
    soup: BeautifulSoup,
    next_zap: int,
    store_order: List[str],
) -> Dict[str, Any]:
    title = extract_title(soup)
    sku = extract_sku(soup)
    description = extract_long_description(soup)
    ean = extract_ean_raw(soup.get_text(" ", strip=True))
    manufacturer = extract_manufacturer(soup)
    image_url = extract_image_url(soup)
    price_w_vat, sale_w_vat, price_wo_vat, sale_wo_vat, em = extract_prices_and_em(
        soup, title, description
    )

    stock = extract_store_stock_from_product_page(soup, store_order)
    if stock:
        dobava = "DA" if any(q > 0 for q in stock.values()) else "NE"
        stock_json = json.dumps(stock, ensure_ascii=False)
    else:
        dobava = extract_delivery_short(soup)
        stock_json = ""

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
            "EM": normalize_em(em) if em else "kos",
            "Valuta": "EUR",
            "DDV": "22",
            "Proizvajalec": manufacturer,
            "Veljavnost od": today_str,
            "Dobava": dobava,
            "Zaloga po poslovalnicah": stock_json,
            "Cena / EM (z DDV)": price_w_vat,
            "Akcijska cena / EM (z DDV)": sale_w_vat,
            "Cena / EM (brez DDV)": price_wo_vat,
            "Akcijska cena / EM (brez DDV)": sale_wo_vat,
            "URL": product_url,
            "SLIKA URL": image_url,
        }
    )

    for store in store_order:
        row[f"Zaloga - {store}"] = stock.get(store, "")

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
    store_order: List[str],
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

    return build_record(product_url, category_name, today_str, soup, next_zap, store_order)


def scrape_slovenijales() -> Tuple[str, str, str]:
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

        store_order = fetch_store_order(session, user_agent, logger)

        batch_rows: List[Dict[str, Any]] = []
        processed_products = 0

        for group_name, category_urls in SLOVENIJALES_CATEGORIES.items():
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
                            store_order=store_order,
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
    scrape_slovenijales()
