import os
from datetime import datetime
from typing import Tuple


def create_output_paths(shop_name: str) -> Tuple[str, str, str, str]:
    """
    Ustvari standardne output poti za scraper.

    Struktura:
        <OUTPUT_DIR>/Ceniki_Scraping/<SHOP>/<YYYY-MM-DD>/

    Če OUTPUT_DIR ni nastavljen, se kot root uporabi mapa,
    v kateri se nahaja zaganjana skripta.

    Vrne:
        json_path, excel_path, log_path, daily_dir
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))

    output_root = os.environ.get("OUTPUT_DIR", project_root)

    today_folder = datetime.now().strftime("%Y-%m-%d")
    filename_date = datetime.now().strftime("%d_%m_%Y")
    log_time = datetime.now().strftime("%H-%M-%S")

    daily_dir = os.path.join(output_root, "Ceniki_Scraping", shop_name, today_folder)
    os.makedirs(daily_dir, exist_ok=True)

    json_path = os.path.join(daily_dir, f"{shop_name}_Podatki_{filename_date}.json")
    excel_path = os.path.join(daily_dir, f"{shop_name}_Podatki_{filename_date}.xlsx")
    log_path = os.path.join(daily_dir, f"{shop_name}_Scraping_Log_{log_time}.txt")

    return json_path, excel_path, log_path, daily_dir
