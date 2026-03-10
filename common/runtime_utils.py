import os
import random
import time


def is_ci() -> bool:
    """
    Preveri, ali scraper teče v GitHub Actions / CI okolju.
    """
    return (
        os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
        or bool(os.environ.get("CI"))
    )


def startup_sleep(
    ci_min: float = 0.5,
    ci_max: float = 3.0,
    local_min: float = 2.0,
    local_max: float = 12.0,
) -> None:
    """
    Naključen začetni zamik, da se scraperji ne zaženejo vedno ob isti sekundi.
    """
    if is_ci():
        time.sleep(random.uniform(ci_min, ci_max))
    else:
        time.sleep(random.uniform(local_min, local_max))


def batch_pause(
    processed_count: int,
    every_n: int,
    pause_min: float,
    pause_max: float,
    logger=None,
) -> None:
    """
    Občasni daljši počitek po določenem številu izdelkov.
    """
    if every_n <= 0:
        return

    if processed_count > 0 and processed_count % every_n == 0:
        wait = random.uniform(pause_min, pause_max)
        if logger:
            logger.log(f"PAUSE: {processed_count} izdelkov -> počitek {wait:.1f}s")
        time.sleep(wait)
