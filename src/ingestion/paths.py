"""Path/key construction helpers shared by download and upload scripts."""
from datetime import date
from pathlib import Path
from typing import Optional


def current_publication(today: Optional[date] = None) -> str:
    """Return the DVF publication label (YYYY-MM) current as of the given date.

    DVF republishes twice a year, in April and October. Any date maps to the most
    recent of those two publication months.
    """
    today = today or date.today()
    year = today.year
    if today.month >= 10:
        return f"{year}-10"
    elif today.month >= 4:
        return f"{year}-04"
    else:
        return f"{year - 1}-10"


def local_path(year: str, dept: str, publication: Optional[str] = None) -> Path:
    publication = publication or current_publication()
    return Path(f"data_raw/{publication}/{year}/{dept}.csv.gz")


def object_key(year: str, dept: str, publication: Optional[str] = None) -> str:
    publication = publication or current_publication()
    return f"dvf/publication={publication}/year={year}/department={dept}/{dept}.csv.gz"