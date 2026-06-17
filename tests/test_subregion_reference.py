from pathlib import Path
import csv


def test_iso3166_2_reference_exists():
    ref_path = (
        Path(__file__).resolve().parent.parent
        / "data"
        / "reference"
        / "subregions"
        / "ISO3166-2.CSV"
    )
    assert ref_path.exists(), "ISO3166-2.CSV should exist"


def test_iso3166_2_reference_contains_known_codes():
    ref_path = (
        Path(__file__).resolve().parent.parent
        / "data"
        / "reference"
        / "subregions"
        / "ISO3166-2.CSV"
    )
    with ref_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        codes = {row["code"] for row in reader if row.get("code")}

    # Known valid ISO 3166-2 subdivisions from analysis baseline.
    for code in ("FR-IDF", "US-PR", "BE-BRU"):
        assert code in codes
