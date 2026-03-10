import json
import os
from typing import List, Dict, Any, Optional

import pandas as pd


def load_existing_data(json_path: str, excel_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Naloži obstoječe podatke.
    Prednost ima JSON, fallback je Excel.

    Vrne seznam dict zapisov.
    """
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
        except Exception:
            pass

    if excel_path and os.path.exists(excel_path):
        try:
            df = pd.read_excel(excel_path)
            return df.to_dict(orient="records")
        except Exception:
            pass

    return []


def make_item_key(item: Dict[str, Any], use_variant: bool = False) -> str:
    """
    Sestavi ključ za dedupe.

    Če use_variant=False:
        ključ = URL

    Če use_variant=True:
        ključ = URL|Varianta
    """
    url = str(item.get("URL") or "").strip()
    if not use_variant:
        return url

    variant = str(item.get("Varianta") or "").strip()
    return f"{url}|{variant}"


def merge_data(
    existing_data: List[Dict[str, Any]],
    new_data: List[Dict[str, Any]],
    use_variant: bool = False,
) -> List[Dict[str, Any]]:
    """
    Združi stare in nove podatke.
    Zadnji zapis z istim ključem zmaga.
    """
    data_dict = {}

    for item in existing_data:
        if not isinstance(item, dict):
            continue
        key = make_item_key(item, use_variant=use_variant)
        if key:
            data_dict[key] = item

    for item in new_data:
        if not isinstance(item, dict):
            continue
        key = make_item_key(item, use_variant=use_variant)
        if key:
            data_dict[key] = item

    merged = list(data_dict.values())

    try:
        merged.sort(key=lambda x: int(x.get("Zap", 0)))
    except Exception:
        pass

    return merged


def save_json(data: List[Dict[str, Any]], json_path: str) -> None:
    """
    Shrani seznam podatkov v JSON.
    """
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_excel(
    data: List[Dict[str, Any]],
    excel_path: str,
    columns: List[str],
    logger=None,
) -> None:
    """
    Zapiše Excel z vnaprej podanimi stolpci.
    Če kateri stolpec manjka, ga doda kot praznega.
    """
    df = pd.DataFrame(data)

    for col in columns:
        if col not in df.columns:
            df[col] = ""

    df[columns].to_excel(excel_path, index=False)

    if logger:
        logger.log("Shranjen Excel.")


def save_data(
    new_data: List[Dict[str, Any]],
    json_path: str,
    excel_path: str,
    columns: List[str],
    logger=None,
    use_variant: bool = False,
) -> List[Dict[str, Any]]:
    """
    Visokonivojska helper funkcija:
    - naloži obstoječe podatke
    - merge
    - shrani JSON
    - zapiše Excel

    Vrne final merged data.
    """
    if not new_data:
        return load_existing_data(json_path, excel_path)

    existing_data = load_existing_data(json_path, excel_path)
    merged = merge_data(existing_data, new_data, use_variant=use_variant)

    save_json(merged, json_path)
    if logger:
        logger.log("Shranjen JSON.")

    write_excel(merged, excel_path, columns, logger=logger)
    return merged


def save_data_batch_json_only(
    new_data: List[Dict[str, Any]],
    json_path: str,
    excel_path: Optional[str] = None,
    logger=None,
    use_variant: bool = False,
) -> List[Dict[str, Any]]:
    """
    Uporabno za checkpoint med scrapingom:
    - merge + save samo JSON
    - Excel se lahko zgradi kasneje 1x na koncu

    Vrne final merged data.
    """
    if not new_data:
        return load_existing_data(json_path, excel_path)

    existing_data = load_existing_data(json_path, excel_path)
    merged = merge_data(existing_data, new_data, use_variant=use_variant)
    save_json(merged, json_path)

    if logger:
        logger.log("Shranjen JSON (batch).")

    return merged


def write_excel_from_json(
    json_path: str,
    excel_path: str,
    columns: List[str],
    logger=None,
) -> None:
    """
    Prebere JSON in zapiše Excel.
    Uporabno, če JSON checkpointaš sproti, Excel pa generiraš 1x na koncu.
    """
    if not os.path.exists(json_path):
        df = pd.DataFrame([], columns=columns)
        df.to_excel(excel_path, index=False)
        if logger:
            logger.log("Shranjen prazen Excel.")
        return

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    write_excel(data, excel_path, columns, logger=logger)


def get_max_zap(data: List[Dict[str, Any]]) -> int:
    """
    Vrne največjo vrednost Zap iz obstoječih podatkov.
    Če podatkov ni ali je napaka, vrne 0.
    """
    try:
        return max((int(x.get("Zap", 0)) for x in data if isinstance(x, dict)), default=0)
    except Exception:
        return 0
