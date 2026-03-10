from datetime import datetime
from typing import Optional, TextIO


class ScraperLogger:
    """
    Enostaven logger za scraperje:
    - piše v console
    - po želji piše tudi v log datoteko
    """

    def __init__(self, log_file: Optional[TextIO] = None):
        self.log_file = log_file

    def log(self, message: str, to_file: bool = True) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"[{timestamp}] {message}"
        print(full_message)

        if to_file and self.log_file:
            try:
                self.log_file.write(full_message + "\n")
                self.log_file.flush()
            except Exception:
                pass

    def close(self) -> None:
        if self.log_file:
            try:
                self.log_file.close()
            except Exception:
                pass
            self.log_file = None


def open_logger(log_path: str, encoding: str = "utf-8") -> ScraperLogger:
    """
    Ustvari logger, ki piše v podano log datoteko.

    Če datoteke ni mogoče odpreti, vrne logger brez file writerja,
    da scraper vseeno normalno teče in loga vsaj v console.
    """
    try:
        log_file = open(log_path, "w", encoding=encoding)
        return ScraperLogger(log_file=log_file)
    except Exception:
        return ScraperLogger(log_file=None)
