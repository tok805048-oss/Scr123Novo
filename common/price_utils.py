import re
from typing import Optional


def parse_float_any(value: str) -> Optional[float]:
    """
    Poskusi pretvoriti različne tekstovne oblike cene v float.

    Primeri vhodov:
    - "42,46"
    - "42.46"
    - "1.234,56"
    - "€ 42,46"
    - "42,46 €"
    - "Akcijska cena: 42,46 €"

    Vrne:
    - float, če uspe
    - None, če ne uspe
    """
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    # odstrani vse razen številk, vejice in pike
    s = re.sub(r"[^\d,\.]", "", s)
    if not s:
        return None

    # primer: 1.234,56
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        # primer: 42,46
        if "," in s and "." not in s:
            s = s.replace(",", ".")

        # če je več pik, predpostavi da so vse razen zadnje tisočice
        if s.count(".") > 1:
            parts = s.split(".")
            s = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(s)
    except Exception:
        return None


def format_price(value: Optional[float]) -> str:
    """
    Formatira float na 2 decimalki z vejico.

    Primer:
    42.456 -> "42,46"
    """
    if value is None:
        return ""
    return f"{value:.2f}".replace(".", ",")


def round_price_2dec(value: str) -> str:
    """
    Vzame tekstovno ceno in jo vrne kot lepo zaokrožen string z 2 decimalkama.

    Primer:
    "42,456" -> "42,46"
    "€ 42,4" -> "42,40"
    """
    parsed = parse_float_any(value)
    return format_price(parsed)


def convert_price_to_without_vat(price_with_vat: str, vat_rate: float) -> str:
    """
    Pretvori ceno z DDV v ceno brez DDV.

    Primer:
    42,46 pri DDV 0.22 -> 34,80
    """
    parsed = parse_float_any(price_with_vat)
    if parsed is None:
        return ""
    return format_price(parsed / (1 + vat_rate))


def extract_first_price(text: str) -> str:
    """
    Iz poljubnega teksta pobere prvo ceno.

    Primer:
    "Akcijska cena 42,46 € Redna cena 49,99 €"
    -> "42,46"
    """
    if not text:
        return ""

    match = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:[.,]\d+)?)", str(text))
    if not match:
        return ""

    return round_price_2dec(match.group(1))


def extract_all_prices(text: str) -> list[str]:
    """
    Iz teksta pobere vse cene in jih vrne kot seznam normaliziranih stringov.

    Primer:
    "Akcijska cena 42,46 € Redna cena 49,99 €"
    -> ["42,46", "49,99"]
    """
    if not text:
        return []

    matches = re.findall(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:[.,]\d+)?)", str(text))
    return [round_price_2dec(m) for m in matches if round_price_2dec(m)]


def extract_price_per_unit(text: str) -> tuple[str, str]:
    """
    Poskusi pobrati ceno na enoto iz oblike:

    - "42,46 € (4,25 €/M2)"
    - "od 39,44 € (4,21 €/M2)"
    - "12,90 € / kos"

    Vrne:
    - (cena, enota)

    Če ne najde cene na enoto:
    - vrne ("", "")
    """
    if not text:
        return "", ""

    t = str(text).replace("\xa0", " ").strip()

    # primer: (4,25 €/M2)
    m = re.search(r"\(\s*([\d\.,]+)\s*€\s*/\s*([A-Za-z0-9²³]+)\s*\)", t)
    if m:
        price = round_price_2dec(m.group(1))
        unit = m.group(2).strip()
        unit = (
            unit.replace("M2", "m2")
            .replace("M3", "m3")
            .replace("M", "m")
            .replace("m²", "m2")
            .replace("m³", "m3")
            .replace("²", "2")
            .replace("³", "3")
        )
        return price, unit

    # primer: 12,90 € / kos
    m2 = re.search(r"([\d\.,]+)\s*€\s*/\s*([A-Za-z0-9²³]+)", t)
    if m2:
        price = round_price_2dec(m2.group(1))
        unit = m2.group(2).strip()
        unit = (
            unit.replace("M2", "m2")
            .replace("M3", "m3")
            .replace("M", "m")
            .replace("m²", "m2")
            .replace("m³", "m3")
            .replace("²", "2")
            .replace("³", "3")
        )
        return price, unit

    return "", ""
