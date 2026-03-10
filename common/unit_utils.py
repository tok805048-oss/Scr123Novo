import re

# Dovoljene / standardne enote mere
ALLOWED_EM = {
    "ar",
    "ha",
    "kam",
    "kg",
    "km",
    "kwh",
    "kw",
    "wat",
    "kpl",
    "kos",
    "kos dan",
    "kos mes",
    "m",
    "m2",
    "m3",
    "cm",
    "kN",
    "km2",
    "kg/m3",
    "kg/h",
    "kg/l",
    "m/dan",
    "m/h",
    "m/min",
    "m/s",
    "m2 dan",
    "m2 mes",
    "m3/dan",
    "m3/h",
    "m3/min",
    "m3/s",
    "m3 d",
    "t",
    "tm",
    "t/dan",
    "t/h",
    "t/let",
    "h",
    "min",
    "s",
    "lit/dan",
    "lit/h",
    "lit/min",
    "lit/s",
    "L",
    "par",
    "pal",
    "sto",
    "skl",
    "del",
    "ključ",
    "os",
    "os d",
    "x",
    "delež",
    "oc",
    "op",
}


def clean_unit_text(unit: str) -> str:
    """
    Osnovno čiščenje enote:
    - odstrani odvečne presledke
    - pretvori m² -> m2, m³ -> m3
    - odstrani odvečne pike/slashe na robu
    """
    if not unit:
        return ""

    u = str(unit).strip()
    u = u.replace("m²", "m2").replace("m³", "m3")
    u = u.replace("²", "2").replace("³", "3")
    u = re.sub(r"\s+", " ", u).strip()
    u = u.replace(".", "").strip().strip("/")

    # normalizacija litrov
    if u == "l":
        u = "L"

    return u


def normalize_em(unit: str) -> str:
    """
    Normalizira enoto mere na dogovorjen zapis.
    Če enota ni prepoznana ali ni v whitelistu, vrne 'kos'.
    """
    if not unit:
        return "kos"

    u = clean_unit_text(unit)
    ul = u.lower()

    # direktni match
    if u in ALLOWED_EM:
        return u
    if ul in ALLOWED_EM:
        return ul

    # pogoste variante
    synonyms = {
        "kosov": "kos",
        "kos.": "kos",
        "kom": "kos",
        "komad": "kos",
        "komadi": "kos",
        "pcs": "kos",
        "pc": "kos",
        "piece": "kos",
        "pak": "kos",
        "paket": "kos",
        "pakiranje": "kos",
        "set": "kpl",
        "komplet": "kpl",
        "m²": "m2",
        "m³": "m3",
        "meter": "m",
        "metrov": "m",
        "tekocih metrov": "m",
        "tekočih metrov": "m",
        "kvadratnih metrov": "m2",
        "kubicnih metrov": "m3",
        "kubičnih metrov": "m3",
        "litrov": "L",
        "ura": "h",
        "ur": "h",
        "dni": "dan",   # če bi kdaj želel to posebej obravnavati
    }

    if ul in synonyms:
        mapped = synonyms[ul]
        if mapped in ALLOWED_EM:
            return mapped

    # posebni patterni
    if ul in ("m2/", "m2/kos", "m2/pak", "m2/pkt"):
        return "m2"
    if ul in ("m3/", "m3/kos", "m3/pak", "m3/pkt"):
        return "m3"
    if ul in ("kg/", "kg/kos", "kg/pak"):
        return "kg"

    # če nič ne paše -> kos
    return "kos"


def is_valid_em(unit: str) -> bool:
    """
    Vrne True, če je enota po normalizaciji v whitelistu.
    """
    if not unit:
        return False
    return normalize_em(unit) in ALLOWED_EM


def guess_em_from_text(text: str) -> str:
    """
    Poskusi uganiti EM iz poljubnega teksta, npr. iz naslova izdelka.

    Primeri:
    - '4m2/pkt' -> m2
    - '10 m2' -> m2
    - '2,25 m3' -> m3
    - '25 kg' -> kg
    - če nič ne najde -> kos
    """
    if not text:
        return "kos"

    t = clean_unit_text(text).lower()

    # najprej bolj specifični patterni
    if re.search(r"\b\d+(?:[.,]\d+)?\s*m2\b", t):
        return "m2"
    if re.search(r"\b\d+(?:[.,]\d+)?\s*m3\b", t):
        return "m3"
    if re.search(r"\b\d+(?:[.,]\d+)?\s*kg\b", t):
        return "kg"
    if re.search(r"\b\d+(?:[.,]\d+)?\s*cm\b", t):
        return "cm"
    if re.search(r"\b\d+(?:[.,]\d+)?\s*km\b", t):
        return "km"
    if re.search(r"\b\d+(?:[.,]\d+)?\s*m\b", t):
        return "m"
    if re.search(r"\b\d+(?:[.,]\d+)?\s*l\b", t):
        return "L"

    # pakirni izrazi
    if "m2/pkt" in t or "m2/pak" in t or "m2/kos" in t:
        return "m2"
    if "m3/pkt" in t or "m3/pak" in t or "m3/kos" in t:
        return "m3"
    if "kg/pak" in t or "kg/kos" in t:
        return "kg"

    return "kos"
