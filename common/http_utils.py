import random
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]


def build_session() -> requests.Session:
    """
    Ustvari requests Session z retry/backoff mehanizmom za tipične
    transient napake (429, 5xx).
    """
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def get_default_headers(
    base_url: str,
    user_agent: str,
    referer: Optional[str] = None,
) -> dict:
    """
    Vrne standardne headerje za scraper requeste.
    """
    return {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sl-SI,sl;q=0.9,en-US;q=0.7,en;q=0.5",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": referer or base_url,
    }


def human_sleep(min_s: float, max_s: float) -> None:
    """
    Naključni sleep med requesti.
    """
    time.sleep(random.uniform(min_s, max_s))


def warmup_session(
    session: requests.Session,
    base_url: str,
    user_agent: str,
    timeout: int = 20,
) -> None:
    """
    Naredi en začetni request na BASE_URL, da se poberejo osnovni cookie-ji/session.
    Če faila, samo tiho nadaljuje.
    """
    headers = get_default_headers(base_url=base_url, user_agent=user_agent, referer=base_url)
    try:
        session.get(base_url, headers=headers, timeout=timeout)
    except Exception:
        pass


def get_page_content(
    session: requests.Session,
    url: str,
    base_url: str,
    user_agent: str,
    referer: Optional[str] = None,
    timeout: int = 25,
    retries: int = 3,
    sleep_min: float = 4.0,
    sleep_max: float = 6.0,
    logger=None,
) -> Optional[str]:
    """
    Standardni GET helper za scraperje.

    Lastnosti:
    - uporablja stabilen UA
    - uporablja referer
    - spoštuje sleep med requesti
    - retry/backoff za 429 in 5xx
    - ne vsebuje agresivne BLOCK/captcha logike
    - vrne None, če po več poskusih ne uspe
    """
    headers = get_default_headers(
        base_url=base_url,
        user_agent=user_agent,
        referer=referer or base_url,
    )

    for attempt in range(1, retries + 1):
        human_sleep(sleep_min, sleep_max)

        try:
            response = session.get(url, headers=headers, timeout=timeout)

            if response.status_code == 429:
                wait = random.uniform(20, 60)
                if logger:
                    logger.log(f"HTTP 429 @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            if response.status_code in (500, 502, 503, 504):
                wait = random.uniform(8, 25)
                if logger:
                    logger.log(f"HTTP {response.status_code} @ {url} -> sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.text

        except Exception as e:
            wait = random.uniform(2, 6) * attempt
            if logger:
                logger.log(f"Napaka pri dostopu {url}: {e} -> sleep {wait:.1f}s")
            time.sleep(wait)

    return None
