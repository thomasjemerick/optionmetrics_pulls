import os
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

def _parse_date(x):
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, str):
        return datetime.strptime(x, "%Y-%m-%d").date()
    return x


@dataclass
class Config:
    # ======================
    # Core date controls
    # ======================
    start_date: str = os.getenv("START_DATE", "2022-01-01")
    end_date: str = os.getenv("END_DATE", "2022-12-31")

    # ======================
    # Storage roots
    # ======================
    root_dir: str = os.getenv(
        "OM_ROOT",
        "/Volumes/Peely SSD/SEHF_data/optionmetrics"
    )

    # ======================
    # Subdirectories
    # ======================
    raw_subdir: str = "raw"
    parquet_subdir: str = "parquet"
    log_subdir: str = "logs"

    # ======================
    # Universe file
    # ======================
    universe_filename: str = os.getenv("UNIVERSE_FILE", "universe_secid.txt")

    def __post_init__(self):
        self.start_date = _parse_date(self.start_date)
        self.end_date = _parse_date(self.end_date)

        self.root_dir = Path(self.root_dir)
        self.raw_dir = self.root_dir / self.raw_subdir
        self.data_dir = self.root_dir / self.parquet_subdir
        self.log_dir = self.root_dir / self.log_subdir

        # Create dirs if missing
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    # ======================
    # Universe handling
    # ======================
    @property
    def universe_path(self) -> Path:
        p = Path(self.universe_filename)
        if p.is_absolute():
            return p
        return Path(__file__).resolve().parent / p

    def load_universe_secids(self) -> list[int]:
        """
        Load SECIDs from universe file (one per line).
        Accepts floats like 101310.0 and converts to int.
        """
        p = self.universe_path
        if not p.exists():
            raise FileNotFoundError(
                f"Universe secid file not found: {p}\n"
                f"Run: python3 01b_build_universe_secid.py"
            )

        secids = []
        for line in p.read_text().splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            secids.append(int(float(s)))

        # Deduplicate, preserve order
        seen = set()
        out = []
        for x in secids:
            if x not in seen:
                out.append(x)
                seen.add(x)

        return out


# Singleton used everywhere
CFG = Config()
