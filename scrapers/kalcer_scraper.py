import json
import os
import random
import re
from datetime import datetime
from itertools import product as cart_product
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

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
    extract_first_price,
    extract_price_per_unit,
    format_price,
    parse_float_any,
    round_price_2dec,
)
from common.runtime_utils import batch_pause, startup_sleep
from common.save_utils import (
    get_max_zap,
    load_existing_data,
    save_data_batch_json_only,
    write_excel_from_json,
)
from common.schema import build_excel_columns, get_base_record
from common.text_utils import (
    clean_multiline_text,
    clean_text,
    extract_art_number,
    extract_between,
    extract_ean_raw,
    safe_truncate,
    unique_preserve_order,
)
from common.unit_utils import guess_em_from_text, normalize_em

SHOP_NAME = "Kalcer"
BASE_URL = "https://www.trgovina-kalcer.si"
DDV_RATE = 0.22

SLEEP_MIN = float(os.environ.get("SCRAPE_SLEEP_MIN", "2.0"))
SLEEP_MAX = float(os.environ.get("SCRAPE_SLEEP_MAX", "5.0"))
BUFFER_FLUSH = int(os.environ.get("BUFFER_FLUSH", "50"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))
MAX_VARIANTS_PER_PRODUCT = int(os.environ.get("MAX_VARIANTS_PER_PRODUCT", "20"))
BREAK_EVERY_PRODUCTS = int(os.environ.get("BREAK_EVERY_PRODUCTS", "150"))
BREAK_SLEEP_MIN = float(os.environ.get("BREAK_SLEEP_MIN", "15"))
BREAK_SLEEP_MAX = float(os.environ.get("BREAK_SLEEP_MAX", "60"))
ENABLE_CART_PROBE = os.environ.get("KALCER_ENABLE_CART_PROBE", "0").lower() in ("1", "true", "yes", "y")

RUN_UA = os.environ.get("SCRAPE_UA") or random.choice(DEFAULT_USER_AGENTS)

STORE_COLS = ["Zaloga - Ljubljana"]
EXCEL_COLS = build_excel_columns(STORE_COLS)

_global_item_counter = 0


# ------------------------------------------------------------
# URL / category filtering
# ------------------------------------------------------------
def is_valid_product_url(url: str) -> bool:
    if not url:
        return False

    if not url.startswith(BASE_URL):
        return False

    parsed = urlparse(url)
    path = parsed.path.rstrip("/").lower()

    blocked_prefixes = (
        "/account",
        "/checkout",
        "/blog",
        "/information",
        "/download",
        "/product/manufacturer",
        "/product/search",
        "/index.php",
    )
    if path.startswith(blocked_prefixes):
        return False

    blocked_exact = {
        "",
        "/",
        "/gradnja",
        "/orodja",
        "/stukature",
        "/kopalnica-wellness",
        "/vrata-okna-stopnice",
        "/leplenje-tesnenje",
    }
    if path in blocked_exact:
        return False

    blocked_contains = (
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "youtube.com",
        "mailto:",
        "tel:",
    )
    if any(x in url.lower() for x in blocked_contains):
        return False

    # produktni URL na Kalcerju nima query parametrov in praviloma ni samo kategorija
    # precej produktov je 1 nivo pod domeno, kategorije pa pogosto več nivojev
    if path.count("/") >= 3 and not re.search(r"-\d+(?:-kg|-mm|-cm|-l)?$", path):
        # to samo po sebi še ni izločitev; pustimo naprej
        pass

    return True


def extract_product_urls_from_category_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: List[str] = []

    # najprej tipični card linki
    for a in soup.select(".product-layout a[href], .product-grid a[href], .product-thumb a[href], .name a[href]"):
        href = a.get("href")
        if not href:
            continue
        full = urljoin(BASE_URL, href)
        if is_valid_product_url(full):
            urls.append(full)

    # fallback: lovi URL-je, ki izgledajo kot produkti
    if not urls:
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(BASE_URL, href)
            if is_valid_product_url(full):
                urls.append(full)

    return unique_preserve_order(urls)


def get_product_links_from_category(session, category_url: str, logger) -> List[str]:
    links: List[str] = []
    prev_fingerprint: Optional[Tuple[str, ...]] = None

    for page in range(1, MAX_PAGES + 1):
        page_url = f"{category_url}?page={page}"
        logger.log(f"  Stran {page}: {page_url}")

        html = get_page_content(
            session=session,
            url=page_url,
            base_url=BASE_URL,
            user_agent=RUN_UA,
            referer=category_url,
            sleep_min=SLEEP_MIN,
            sleep_max=SLEEP_MAX,
            logger=logger,
        )
        if not html:
            break

        page_urls = extract_product_urls_from_category_html(html)
        if not page_urls:
            break

        fingerprint = tuple(page_urls[:10])
        if prev_fingerprint is not None and fingerprint == prev_fingerprint:
            logger.log(f"  Stran {page} se ponavlja -> zaključujem kategorijo.")
            break
        prev_fingerprint = fingerprint

        before = len(links)
        for u in page_urls:
            if u not in links:
                links.append(u)

        # če se nič novega ne doda, verjetno konec
        if len(links) == before:
            break

    return links


# ------------------------------------------------------------
# Product helpers
# ------------------------------------------------------------
def extract_image_url(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return clean_text(og["content"])

    tw = soup.find("meta", attrs={"name": "twitter:image"})
    if tw and tw.get("content"):
        return clean_text(tw["content"])

    for img in soup.select("img[src]"):
        src = img.get("src", "").strip()
        if not src:
            continue
        src_full = urljoin(BASE_URL, src)
        low = src_full.lower()

        # izloči social / icon / logo slike
        if any(x in low for x in ("/image/icons/", "/catalog/demo/", "fb-icon", "logo", "instagram", "facebook")):
            continue
        return src_full

    return ""


def extract_product_long_description(soup: BeautifulSoup) -> str:
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    lines = [clean_text(x) for x in txt.splitlines() if clean_text(x)]

    # najbolj tipične stop sekcije
    stop_headers = {
        "sorodni izdelki",
        "podobni izdelki",
        "mnenja",
        "ocene",
        "prijava",
        "košarica",
        "opis",
    }

    # če najdemo "Tehnični podatki", vzamemo še to
    out: List[str] = []
    capture = False
    for line in lines:
        low = line.lower()

        if not capture and len(line) > 20:
            capture = True

        if capture:
            if low in stop_headers:
                break
            out.append(line)

    return safe_truncate("\n".join(out), 8000)


def extract_brand(soup: BeautifulSoup, page_text: str) -> str:
    # tipične povezave proizvajalca
    for a in soup.select('a[href*="/manufacturer"], a[href*="/m-"]'):
        txt = clean_text(a.get_text(" ", strip=True))
        if txt and len(txt) <= 80:
            return txt

    m = re.search(r"Proizvajalec\s*[:\-]\s*(.+)", page_text, flags=re.IGNORECASE)
    if m:
        return clean_text(m.group(1))

    return ""


def extract_delivery_and_stock(page_text: str) -> Tuple[str, str, Dict[str, str]]:
    text = page_text.lower()

    store_stock: Dict[str, str] = {}

    # Kalcer tipično omenja Ljubljano; prazno ni zaloga
    if "ljubljana" in text and "na zalogi" in text:
        store_stock["Ljubljana"] = "DA"
    elif "ljubljana" in text and "ni na zalogi" in text:
        store_stock["Ljubljana"] = "NE"

    # dobava
    delivery = ""
    if "na zalogi" in text:
        delivery = "DA"
    elif "ni na zalogi" in text or "trenutno ni na zalogi" in text:
        delivery = "NE"
    else:
        m = re.search(r"(\d+\s*[-–]\s*\d+\s*delovnih\s*dni)", text, flags=re.IGNORECASE)
        if m:
            delivery = clean_text(m.group(1))

    stock_json = json.dumps(store_stock, ensure_ascii=False) if store_stock else ""
    return delivery, stock_json, store_stock


def extract_product_id(html: str, soup: BeautifulSoup) -> str:
    el = soup.select_one('input[name="product_id"]')
    if el and el.get("value"):
        return clean_text(el["value"])

    m = re.search(r'name="product_id"\s+value="(\d+)"', html)
    if m:
        return clean_text(m.group(1))

    m = re.search(r"product_id\s*[:=]\s*['\"](\d+)['\"]", html)
    if m:
        return clean_text(m.group(1))

    return ""


def extract_option_groups(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    groups: List[Dict[str, Any]] = []

    for sel in soup.select('select[name^="option["]'):
        name = sel.get("name")
        label = ""
        fg = sel.find_parent(class_="form-group")
        if fg:
            lab = fg.select_one("label")
            if lab:
                label = clean_text(lab.get_text(" ", strip=True))

        values = []
        for opt in sel.select("option"):
            val = clean_text(opt.get("value", ""))
            txt = clean_text(opt.get_text(" ", strip=True))
            if val and val != "0":
                values.append({"id": val, "text": txt})

        if name and values:
            groups.append({"name": name, "label": label or name, "values": values})

    if not groups:
        radios = soup.select('input[type="radio"][name^="option["]')
        bucket: Dict[str, Dict[str, Any]] = {}
        for r in radios:
            name = r.get("name")
            val = clean_text(r.get("value", ""))
            if not name or not val:
                continue

            label = name
            txt = ""
            parent_label = r.find_parent("label")
            if parent_label:
                txt = clean_text(parent_label.get_text(" ", strip=True))

            bucket.setdefault(name, {"name": name, "label": label, "values": []})
            bucket[name]["values"].append({"id": val, "text": txt or val})

        groups = list(bucket.values())

    return groups


def try_price_from_option_text(base_price: str, option_text: str) -> str:
    base_val = parse_float_any(base_price)
    if base_val is None:
        return ""

    m = re.search(r"([+-]\s*\d{1,3}(?:\.\d{3})*,\d{2})\s*€", option_text)
    if not m:
        return ""

    mod_val = parse_float_any(m.group(1))
    if mod_val is None:
        return ""

    return format_price(base_val + mod_val)


def extract_best_price_and_em(soup: BeautifulSoup, page_text: str) -> Tuple[str, str, str]:
    """
    Vrne:
    - redna cena
    - akcijska cena
    - EM
    """
    regular = ""
    special = ""
    em = ""

    # najprej lovi cene na enoto
    for candidate in [
        page_text,
        soup.get_text(" ", strip=True),
    ]:
        price_per_em, unit = extract_price_per_unit(candidate)
        if price_per_em:
            regular = round_price_2dec(price_per_em)
            em = normalize_em(unit)
            break

    # fallback na vse cene
    prices = extract_all_prices(page_text)
    if not regular and prices:
        regular = prices[0]

    # OpenCart-like selektorji
    special_el = soup.select_one(".price-new, .productSpecialPrice")
    old_el = soup.select_one(".price-old, .price-old, .old-price")

    if special_el:
        sp = extract_first_price(special_el.get_text(" ", strip=True))
        if sp:
            special = sp

    if old_el:
        rg = extract_first_price(old_el.get_text(" ", strip=True))
        if rg:
            regular = rg

    # če je akcijska cena, a regular manjka, regular pusti prazen
    if not em:
        em = normalize_em(guess_em_from_text(page_text))

    return regular, special, em


def build_product_record(url: str, cat_name: str, date_str: str) -> Dict[str, Any]:
    data = get_base_record()
    data["Skupina"] = cat_name
    data["URL"] = url
    data["Veljavnost od"] = date_str
    return data


def extract_product_details(session, url: str, cat_name: str, date_str: str, logger) -> List[Dict[str, Any]]:
    global _global_item_counter

    logger.log(f"    - Detajli: {url}")

    html = get_page_content(
        session=session,
        url=url,
        base_url=BASE_URL,
        user_agent=RUN_UA,
        referer=url,
        sleep_min=SLEEP_MIN,
        sleep_max=SLEEP_MAX,
        logger=logger,
    )
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text("\n", strip=True).replace("\xa0", " ")

    title = ""
    h1 = soup.select_one("h1.product-name") or soup.select_one("h1")
    if h1:
        title = clean_text(h1.get_text(" ", strip=True))

    # minimalna validacija: ne sprejmi sistemskih strani
    if not title or title.lower() in {"prijava", "košarica", "blog", "splošni pogoji"}:
        return []

    data = build_product_record(url, cat_name, date_str)
    data["Opis"] = title
    data["Opis izdelka"] = extract_product_long_description(soup)
    data["EAN"] = extract_ean_raw(page_text)
    data["Oznaka / naziv"] = extract_art_number(page_text)
    data["Proizvajalec"] = extract_brand(soup, page_text)
    data["SLIKA URL"] = extract_image_url(soup)

    regular, special, em = extract_best_price_and_em(soup, page_text)
    data["Cena / EM (z DDV)"] = regular
    data["Akcijska cena / EM (z DDV)"] = special
    data["EM"] = normalize_em(em or guess_em_from_text(title))

    if data["Cena / EM (z DDV)"]:
        data["Cena / EM (brez DDV)"] = convert_price_to_without_vat(data["Cena / EM (z DDV)"], DDV_RATE)
    if data["Akcijska cena / EM (z DDV)"]:
        data["Akcijska cena / EM (brez DDV)"] = convert_price_to_without_vat(
            data["Akcijska cena / EM (z DDV)"], DDV_RATE
        )

    delivery, stock_json, stock_map = extract_delivery_and_stock(page_text)
    data["Dobava"] = delivery
    data["Zaloga po centrih"] = stock_json
    data["Zaloga - Ljubljana"] = stock_map.get("Ljubljana", "")

    # variante
    option_groups = extract_option_groups(soup)
    if not option_groups:
        _global_item_counter += 1
        data["Zap"] = _global_item_counter
        return [data]

    combos = 1
    for g in option_groups:
        combos *= len(g["values"])
    if combos > MAX_VARIANTS_PER_PRODUCT:
        _global_item_counter += 1
        data["Zap"] = _global_item_counter
        return [data]

    product_id = extract_product_id(html, soup)
    results: List[Dict[str, Any]] = []

    value_lists = [
        [(g["name"], v["id"], g["label"], v["text"]) for v in g["values"]]
        for g in option_groups
    ]

    for combo in cart_product(*value_lists):
        rec = dict(data)
        variant_label = ", ".join(
            [f"{lab}: {txt}".strip(": ") for (_, _, lab, txt) in combo if clean_text(txt)]
        )
        rec["Varianta"] = variant_label

        variant_price = ""
        for (_, _, _, txt) in combo:
            variant_price = try_price_from_option_text(rec["Cena / EM (z DDV)"], txt)
            if variant_price:
                break

        if variant_price:
            rec["Cena / EM (z DDV)"] = variant_price
            rec["Cena / EM (brez DDV)"] = convert_price_to_without_vat(variant_price, DDV_RATE)

        _global_item_counter += 1
        rec["Zap"] = _global_item_counter
        results.append(rec)

    return results


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main() -> None:
    global _global_item_counter

    startup_sleep()

    json_path, excel_path, log_path, _ = create_output_paths(SHOP_NAME)
    logger = open_logger(log_path)

    logger.log(f"--- Zagon {SHOP_NAME} ---")
    logger.log(f"UA: {RUN_UA}")
    logger.log(f"JSON:  {json_path}")
    logger.log(f"Excel: {excel_path}")
    logger.log(f"Log:   {log_path}")

    session = build_session()
    warmup_session(session, BASE_URL, RUN_UA)

    existing = load_existing_data(json_path, excel_path)
    _global_item_counter = get_max_zap(existing)

    date_str = datetime.now().strftime("%Y-%m-%d")
    buffer: List[Dict[str, Any]] = []
    processed = 0

    try:
        for main_cat, urls in KALCER_CATEGORIES.items():
            logger.log(f"\n--- {main_cat} ---")
            for category_url in urls:
                sub_name = category_url.rstrip("/").split("/")[-1]
                logger.log(f"\n  Kategorija: {sub_name}")

                product_links = get_product_links_from_category(session, category_url, logger)

                for link in product_links:
                    rows = extract_product_details(session, link, sub_name, date_str, logger)
                    if rows:
                        buffer.extend(rows)
                        processed += len(rows)

                    if len(buffer) >= BUFFER_FLUSH:
                        save_data_batch_json_only(
                            new_data=buffer,
                            json_path=json_path,
                            excel_path=excel_path,
                            logger=logger,
                            use_variant=True,
                        )
                        buffer = []

                    batch_pause(
                        processed_count=processed,
                        every_n=BREAK_EVERY_PRODUCTS,
                        pause_min=BREAK_SLEEP_MIN,
                        pause_max=BREAK_SLEEP_MAX,
                        logger=logger,
                    )

                if buffer:
                    save_data_batch_json_only(
                        new_data=buffer,
                        json_path=json_path,
                        excel_path=excel_path,
                        logger=logger,
                        use_variant=True,
                    )
                    buffer = []

    except Exception as e:
        logger.log(f"NAPAKA: {e}")
    finally:
        if buffer:
            save_data_batch_json_only(
                new_data=buffer,
                json_path=json_path,
                excel_path=excel_path,
                logger=logger,
                use_variant=True,
            )

        write_excel_from_json(
            json_path=json_path,
            excel_path=excel_path,
            columns=EXCEL_COLS,
            logger=logger,
        )
        logger.close()


if __name__ == "__main__":
    main()
