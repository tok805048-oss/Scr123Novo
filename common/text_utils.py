import re
from typing import List, Optional


def clean_text(value: str) -> str:
    """
    Osnovno čiščenje besedila:
    - None -> ""
    - odstrani odvečne whitespace
    - zamenja NBSP
    """
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_multiline_text(value: str) -> str:
    """
    Očisti multiline tekst:
    - ohrani vrstice
    - počisti prazne / odvečne whitespace
    """
    if value is None:
        return ""

    text = str(value).replace("\xa0", " ")
    lines = text.splitlines()
    cleaned = []

    for line in lines:
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            cleaned.append(line)

    return "\n".join(cleaned)


def safe_truncate(text: str, max_len: int = 8000) -> str:
    """
    Varno skrajša tekst za Excel / JSON.
    """
    text = clean_multiline_text(text)
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "…"


def extract_first_regex(text: str, pattern: str, flags: int = 0, group: int = 1) -> str:
    """
    Vrne prvi regex match ali "".
    """
    if not text:
        return ""
    m = re.search(pattern, text, flags=flags)
    if not m:
        return ""
    try:
        return clean_text(m.group(group))
    except Exception:
        return ""


def extract_all_regex(text: str, pattern: str, flags: int = 0, group: int = 1) -> List[str]:
    """
    Vrne vse regex matche kot list stringov.
    """
    if not text:
        return []

    matches = re.findall(pattern, text, flags)
    out = []

    for m in matches:
        if isinstance(m, tuple):
            try:
                val = m[group - 1]
            except Exception:
                continue
        else:
            val = m

        val = clean_text(val)
        if val:
            out.append(val)

    return out


def extract_between(text: str, start_label: str, end_labels: List[str]) -> str:
    """
    Izreže tekst med začetno labelo in najbližjo končno labelo.

    Primer:
        extract_between(page_text, "Kratek opis", ["Podrobnosti", "Več informacij"])
    """
    if not text or start_label not in text:
        return ""

    start = text.find(start_label) + len(start_label)
    sub = text[start:]

    end_pos = len(sub)
    for label in end_labels:
        idx = sub.find(label)
        if idx != -1 and idx < end_pos:
            end_pos = idx

    return clean_multiline_text(sub[:end_pos].strip(" \n\r\t:-"))


def extract_ean_raw(text: str) -> str:
    """
    Poberi EAN / GTIN kot je zapisan.
    Ne validira dolžine, samo pobere številke.
    """
    if not text:
        return ""

    patterns = [
        r"\bEAN\b\s*[:#]?\s*([0-9]{6,20})",
        r"\bGTIN\b\s*[:#]?\s*([0-9]{6,20})",
        r"\bEan\b\s*[:#]?\s*([0-9]{6,20})",
    ]

    for pattern in patterns:
        val = extract_first_regex(text, pattern, flags=re.IGNORECASE)
        if val:
            return val

    return ""


def extract_art_number(text: str) -> str:
    """
    Poberi številko artikla brez odvečnega teksta.

    Primeri:
    - 'Št. art.: 3060449' -> '3060449'
    - 'art: 12345' -> '12345'
    """
    if not text:
        return ""

    patterns = [
        r"Št\.?\s*art\.?\s*:\s*([0-9A-Za-z\-_\/]+)",
        r"\bart\.?\s*:\s*([0-9A-Za-z\-_\/]+)",
        r"\bšifra\b\s*[:#]?\s*([0-9A-Za-z\-_\/]+)",
        r"\bkoda\b\s*[:#]?\s*([0-9A-Za-z\-_\/]+)",
        r"\bsku\b\s*[:#]?\s*([0-9A-Za-z\-_\/]+)",
    ]

    for pattern in patterns:
        val = extract_first_regex(text, pattern, flags=re.IGNORECASE)
        if val:
            return val

    return ""


def strip_art_prefix(value: str) -> str:
    """
    Če je v tekstu še vedno 'Št. art.: 12345', odstrani prefix.
    """
    if not value:
        return ""

    value = clean_text(value)
    value = re.sub(r"^Št\.?\s*art\.?\s*:\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^art\.?\s*:\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^šifra\s*:\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^koda\s*:\s*", "", value, flags=re.IGNORECASE)
    return clean_text(value)


def extract_manufacturer(text: str) -> str:
    """
    Best-effort manufacturer extraction iz navadnega teksta.
    """
    if not text:
        return ""

    patterns = [
        r"\bProizvajalec\b\s*[:#]?\s*([^\n\r]+)",
        r"\bManufacturer\b\s*[:#]?\s*([^\n\r]+)",
        r"\bBrand\b\s*[:#]?\s*([^\n\r]+)",
    ]

    for pattern in patterns:
        val = extract_first_regex(text, pattern, flags=re.IGNORECASE)
        if val:
            # odsekaj tipične naslednje labele, če se zgodi, da je vse v eni vrstici
            val = re.split(r"\b(Šifra|Koda|EAN|GTIN|Tehnične lastnosti|Opis)\b", val, maxsplit=1)[0].strip()
            return clean_text(val)

    return ""


def unique_preserve_order(items: List[str]) -> List[str]:
    """
    Odstrani duplikate, ohrani vrstni red.
    """
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out
