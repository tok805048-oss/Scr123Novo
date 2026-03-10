from typing import Dict, List, Any


BASE_EXCEL_COLS: List[str] = [
    "Skupina",
    "Zap",
    "Oznaka / naziv",
    "EAN",
    "Opis",
    "Opis izdelka",
    "Varianta",
    "EM",
    "Valuta",
    "DDV",
    "Proizvajalec",
    "Veljavnost od",
    "Dobava",
    "Zaloga po centrih",
    "Cena / EM (z DDV)",
    "Akcijska cena / EM (z DDV)",
    "Cena / EM (brez DDV)",
    "Akcijska cena / EM (brez DDV)",
    "URL",
    "SLIKA URL",
]


def get_base_record() -> Dict[str, Any]:
    """
    Vrne prazen osnovni zapis za scraperje.
    Uporabno kot template, da imajo vsi scraperji enako osnovo.
    """
    return {
        "Skupina": "",
        "Zap": 0,
        "Oznaka / naziv": "",
        "EAN": "",
        "Opis": "",
        "Opis izdelka": "",
        "Varianta": "",
        "EM": "kos",
        "Valuta": "EUR",
        "DDV": "22",
        "Proizvajalec": "",
        "Veljavnost od": "",
        "Dobava": "",
        "Zaloga po centrih": "",
        "Cena / EM (z DDV)": "",
        "Akcijska cena / EM (z DDV)": "",
        "Cena / EM (brez DDV)": "",
        "Akcijska cena / EM (brez DDV)": "",
        "URL": "",
        "SLIKA URL": "",
    }


def build_excel_columns(extra_columns: List[str] = None) -> List[str]:
    """
    Sestavi končni seznam Excel stolpcev:
    - najprej osnovni stolpci
    - nato dodatni stolpci, npr. 'Zaloga - Ljubljana', 'Zaloga - Maribor'
    """
    extra_columns = extra_columns or []

    cols = list(BASE_EXCEL_COLS)
    for col in extra_columns:
        if col not in cols:
            cols.append(col)

    return cols


def merge_extra_columns_from_data(data: List[Dict[str, Any]]) -> List[str]:
    """
    Iz podatkov pobere dodatne stolpce, ki niso v BASE_EXCEL_COLS.
    To je uporabno predvsem za dinamične centre / poslovalnice.

    Primer:
    - Zaloga - Ljubljana
    - Zaloga - Maribor
    """
    extra_cols = set()

    for row in data:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            if key not in BASE_EXCEL_COLS:
                extra_cols.add(key)

    return sorted(extra_cols)
