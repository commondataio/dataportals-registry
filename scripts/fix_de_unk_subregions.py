#!/usr/bin/env python3
"""
Fix DE-UNK records: update owner subregion to the real one and move to proper dirs.

Records in data/entities/DE/DE-UNK with owner subregion DE-UNK are reviewed.
Real subregion is inferred from link domain, record id, owner name, and description.
"""

from __future__ import annotations

from pathlib import Path

import yaml

BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
DE_UNK_DIR = ENTITIES_DIR / "DE" / "DE-UNK"

# German subregion id -> display name (ISO 3166-2)
DE_SUBREGION_NAMES = {
    "DE-BW": "Baden-Wurttemberg",
    "DE-BY": "Bayern",
    "DE-BE": "Berlin",
    "DE-BB": "Brandenburg",
    "DE-HB": "Bremen",
    "DE-HH": "Hamburg",
    "DE-HE": "Hessen",
    "DE-MV": "Mecklenburg-Vorpommern",
    "DE-NI": "Niedersachsen",
    "DE-NW": "Nordrhein-Westfalen",
    "DE-RP": "Rheinland-Pfalz",
    "DE-SL": "Saarland",
    "DE-SN": "Sachsen",
    "DE-ST": "Sachsen-Anhalt",
    "DE-SH": "Schleswig-Holstein",
    "DE-TH": "Thuringen",
}

# catalog id -> (subregion_id, catalog_type_subdir)
# Federal = move to DE/Federal/
ID_TO_SUBREGION: dict[str, tuple[str, str]] = {
    # opendata
    "wwwdatenportalmuensterlandde": ("DE-NW", "opendata"),  # Münsterland
    "opendatavagde": ("DE-BY", "opendata"),  # VAG Nürnberg
    "opendatadortmundde": ("DE-NW", "opendata"),  # Dortmund
    "katalogunseregelderde": ("Federal", "opendata"),  # nationwide
    "geoportalmonheimde": ("DE-NW", "geo"),  # Monheim am Rhein
    # geo
    "wwwwaldgeoportalde": ("DE-ST", "geo"),  # Sachsen-Anhalt
    "wwwlandkreismolde": ("DE-BB", "geo"),  # Märkisch-Oderland
    "wmsleipzigde": ("DE-SN", "geo"),  # Leipzig
    "wmsfiswassermvde": ("DE-MV", "geo"),  # fis-wasser-mv
    "wesselingwheregroupcom": ("DE-NW", "geo"),  # Wesseling
    "umapdmhode": ("Federal", "geo"),  # dmho - unclear
    "stadtplanweimarde": ("DE-TH", "geo"),  # Weimar
    "stadtplantroisdorfde": ("DE-NW", "geo"),  # Troisdorf
    "smartpublicsafetyhubblaulichthubarcgiscom": ("Federal", "geo"),  # multi-city
    "servicekreisohde": ("DE-SH", "geo"),  # Ostholstein
    "rudolstadtgajamatrixde": ("DE-TH", "geo"),  # Rudolstadt
    "plisbbde": ("DE-BB", "geo"),  # plis-bb
    "metaverde": ("Federal", "geo"),  # Metaver
    "mapskreisborkende": ("DE-NW", "geo"),  # Kreis Borken
    "mapgalleryarlgishubarcgiscom": ("DE-BY", "geo"),  # ArlGIS Alpine
    "grosskopfbuergergisde": ("Federal", "geo"),  # unclear
    "gisschleswigflensburgde": ("DE-SH", "geo"),
    "gisrheinhunsrueckde": ("DE-RP", "geo"),
    "gisplanungsregionabwde": ("DE-BW", "geo"),  # Albstadt-Balingen-Waldachtal
    "giskreisploende": ("DE-SH", "geo"),
    "gisherzogtumlauenburgde": ("DE-SH", "geo"),
    "gisbottropde": ("DE-NW", "geo"),
    "geowebdiepholzde": ("DE-NI", "geo"),
    "geoservergeonetmrnde": ("DE-BW", "geo"),  # Rhein-Neckar, Mannheim
    "geoportalumweltsachsende": ("DE-SN", "geo"),
    "geoportalduisburgde": ("DE-NW", "geo"),
    "geoportalbirkenwerderde": ("DE-BB", "geo"),
    "geoportalbbvdeutschlandde": ("DE-BY", "geo"),  # Bayerischer Bauernverband
    "geoportal2kreispinnebergde": ("DE-SH", "geo"),
    "geomendende": ("DE-NW", "geo"),
    "geodatenhernede": ("DE-NW", "geo"),
    "geo2maerkischerkreisde": ("DE-NW", "geo"),
    "gdistadtbrandenburgde": ("DE-BB", "geo"),
    "gdidiepholzde": ("DE-NI", "geo"),
    "elsterwerdagajamatrixde": ("DE-BB", "geo"),
    "cardocottbusde": ("DE-BB", "geo"),
    "baysisbayernde": ("DE-BY", "geo"),
}


def main() -> None:
    dry_run = "--dry-run" in __import__("sys").argv
    if dry_run:
        print("DRY RUN - no files will be moved\n")

    to_move: list[tuple[Path, str, str, dict]] = []  # (src, subregion, type, data)

    for yaml_path in sorted(DE_UNK_DIR.rglob("*.yaml")):
        try:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  Skip {yaml_path.name}: {e}")
            continue
        if not isinstance(data, dict):
            continue

        catalog_id = (data.get("id") or "").strip()
        if not catalog_id:
            print(f"  Skip {yaml_path.name}: no id")
            continue

        mapping = ID_TO_SUBREGION.get(catalog_id)
        if not mapping:
            print(f"  Skip {yaml_path.name}: no mapping for id={catalog_id}")
            continue

        subregion_id, catalog_type = mapping
        to_move.append((yaml_path, subregion_id, catalog_type, data))

    print(f"Found {len(to_move)} records to move:\n")
    by_target: dict[str, list[tuple[Path, dict]]] = {}
    for path, subregion, _, record in to_move:
        key = f"DE/{subregion}" if subregion != "Federal" else "DE/Federal"
        by_target.setdefault(key, []).append((path, record))

    for target in sorted(by_target.keys()):
        items = by_target[target]
        print(f"  {target} ({len(items)}):")
        for path, record in items[:5]:
            print(f"    - {path.name}")
        if len(items) > 5:
            print(f"    ... and {len(items) - 5} more")
        print()

    if dry_run or not to_move:
        return

    for src_path, subregion_id, catalog_type, data in to_move:
        if subregion_id == "Federal":
            target_dir = ENTITIES_DIR / "DE" / "Federal" / catalog_type
        else:
            target_dir = ENTITIES_DIR / "DE" / subregion_id / catalog_type

        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / src_path.name

        # Update owner subregion in data
        owner = data.get("owner", {}) or {}
        loc = owner.get("location", {}) or {}
        if subregion_id != "Federal":
            loc["subregion"] = {
                "id": subregion_id,
                "name": DE_SUBREGION_NAMES.get(subregion_id, subregion_id),
            }
        else:
            # Federal: remove subregion or set level to 20
            loc.pop("subregion", None)
            if "level" in loc and loc.get("level") == 30:
                loc["level"] = 20
        owner["location"] = loc
        data["owner"] = owner

        target_path.write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        src_path.unlink()
        print(f"Moved {src_path.name} -> DE/{subregion_id}/{catalog_type}/")

    print(f"\nMoved {len(to_move)} files. Run: python scripts/builder.py validate-yaml")


if __name__ == "__main__":
    main()
