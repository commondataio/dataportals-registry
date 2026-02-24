#!/usr/bin/env python3
"""
Move FR-UNK records to proper subregion directories based on place names,
department numbers, and owner/link content.

French regions (ISO 3166-2): FR-ARA, FR-BFC, FR-BRE, FR-CVL, FR-20R, FR-GES,
FR-HDF, FR-IDF, FR-NOR, FR-NAQ, FR-OCC, FR-PDL, FR-PAC
Overseas: FR-971 (Guadeloupe), FR-972 (Martinique), FR-974 (Réunion),
FR-GF (Guyane), FR-NC (Nouvelle-Calédonie), FR-PF (Polynésie), FR-YT (Mayotte)
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import yaml

ENTITIES_DIR = Path(__file__).resolve().parents[1] / "data" / "entities"
FR_UNK = ENTITIES_DIR / "FR" / "FR-UNK"

# Department number -> region (metropolitan France)
DEPT_TO_REGION: dict[str, str] = {
    "01": "FR-ARA",  # Ain
    "02": "FR-HDF",  # Aisne
    "03": "FR-ARA",  # Allier
    "04": "FR-PAC",  # Alpes-de-Haute-Provence
    "05": "FR-PAC",  # Hautes-Alpes
    "06": "FR-PAC",  # Alpes-Maritimes
    "07": "FR-ARA",  # Ardèche
    "08": "FR-GES",  # Ardennes
    "09": "FR-OCC",  # Ariège
    "10": "FR-GES",  # Aube
    "11": "FR-OCC",  # Aude
    "12": "FR-OCC",  # Aveyron
    "13": "FR-PAC",  # Bouches-du-Rhône
    "14": "FR-NOR",  # Calvados
    "15": "FR-ARA",  # Cantal
    "16": "FR-NAQ",  # Charente
    "17": "FR-NAQ",  # Charente-Maritime
    "18": "FR-CVL",  # Cher
    "19": "FR-NAQ",  # Corrèze
    "21": "FR-BFC",  # Côte-d'Or
    "22": "FR-BRE",  # Côtes-d'Armor
    "23": "FR-NAQ",  # Creuse
    "24": "FR-NAQ",  # Dordogne
    "25": "FR-BFC",  # Doubs
    "26": "FR-ARA",  # Drôme
    "27": "FR-NOR",  # Eure
    "28": "FR-CVL",  # Eure-et-Loir
    "29": "FR-BRE",  # Finistère
    "30": "FR-OCC",  # Gard
    "31": "FR-OCC",  # Haute-Garonne
    "32": "FR-OCC",  # Gers
    "33": "FR-NAQ",  # Gironde
    "34": "FR-OCC",  # Hérault
    "35": "FR-BRE",  # Ille-et-Vilaine
    "36": "FR-CVL",  # Indre
    "37": "FR-CVL",  # Indre-et-Loire
    "38": "FR-ARA",  # Isère
    "39": "FR-BFC",  # Jura
    "40": "FR-NAQ",  # Landes
    "41": "FR-CVL",  # Loir-et-Cher
    "42": "FR-ARA",  # Loire
    "43": "FR-ARA",  # Haute-Loire
    "44": "FR-PDL",  # Loire-Atlantique
    "45": "FR-CVL",  # Loiret
    "46": "FR-OCC",  # Lot
    "47": "FR-NAQ",  # Lot-et-Garonne
    "48": "FR-OCC",  # Lozère
    "49": "FR-PDL",  # Maine-et-Loire
    "50": "FR-NOR",  # Manche
    "51": "FR-GES",  # Marne
    "52": "FR-GES",  # Haute-Marne
    "53": "FR-PDL",  # Mayenne
    "54": "FR-GES",  # Meurthe-et-Moselle
    "55": "FR-GES",  # Meuse
    "56": "FR-BRE",  # Morbihan
    "57": "FR-GES",  # Moselle
    "58": "FR-BFC",  # Nièvre
    "59": "FR-HDF",  # Nord
    "60": "FR-HDF",  # Oise
    "61": "FR-NOR",  # Orne
    "62": "FR-HDF",  # Pas-de-Calais
    "63": "FR-ARA",  # Puy-de-Dôme
    "64": "FR-NAQ",  # Pyrénées-Atlantiques
    "65": "FR-OCC",  # Hautes-Pyrénées
    "66": "FR-OCC",  # Pyrénées-Orientales
    "67": "FR-GES",  # Bas-Rhin
    "68": "FR-GES",  # Haut-Rhin
    "69": "FR-ARA",  # Rhône
    "70": "FR-BFC",  # Haute-Saône
    "71": "FR-BFC",  # Saône-et-Loire
    "72": "FR-PDL",  # Sarthe
    "73": "FR-ARA",  # Savoie
    "74": "FR-ARA",  # Haute-Savoie
    "75": "FR-IDF",  # Paris
    "76": "FR-NOR",  # Seine-Maritime
    "77": "FR-IDF",  # Seine-et-Marne
    "78": "FR-IDF",  # Yvelines
    "79": "FR-NAQ",  # Deux-Sèvres
    "80": "FR-HDF",  # Somme
    "81": "FR-OCC",  # Tarn
    "82": "FR-OCC",  # Tarn-et-Garonne
    "83": "FR-PAC",  # Var
    "84": "FR-PAC",  # Vaucluse
    "85": "FR-PDL",  # Vendée
    "86": "FR-NAQ",  # Vienne
    "87": "FR-NAQ",  # Haute-Vienne
    "88": "FR-GES",  # Vosges
    "89": "FR-BFC",  # Yonne
    "90": "FR-BFC",  # Territoire de Belfort
    "91": "FR-IDF",  # Essonne
    "92": "FR-IDF",  # Hauts-de-Seine
    "93": "FR-IDF",  # Seine-Saint-Denis
    "94": "FR-IDF",  # Val-de-Marne
    "95": "FR-IDF",  # Val-d'Oise
}

REGION_NAMES: dict[str, str] = {
    "FR-ARA": "Auvergne-Rhône-Alpes",
    "FR-BFC": "Bourgogne-Franche-Comté",
    "FR-BRE": "Bretagne",
    "FR-CVL": "Centre-Val de Loire",
    "FR-20R": "Corse",
    "FR-GES": "Grand Est",
    "FR-HDF": "Hauts-de-France",
    "FR-IDF": "Île-de-France",
    "FR-NOR": "Normandie",
    "FR-NAQ": "Nouvelle-Aquitaine",
    "FR-OCC": "Occitanie",
    "FR-PDL": "Pays de la Loire",
    "FR-PAC": "Provence-Alpes-Côte d'Azur",
    "FR-971": "Guadeloupe",
    "FR-972": "Martinique",
    "FR-974": "La Réunion",
    "FR-GF": "Guyane",
    "FR-NC": "Nouvelle-Calédonie",
    "FR-PF": "Polynésie française",
    "FR-YT": "Mayotte",
}

# Place/city/region name tokens -> region
PLACE_TO_REGION: dict[str, str] = {
    "lorient": "FR-BRE",
    "lorientagglo": "FR-BRE",
    "bzh": "FR-BRE",
    "bretagne": "FR-BRE",
    "breizh": "FR-BRE",
    "finistere": "FR-BRE",
    "morbihan": "FR-BRE",
    "rennes": "FR-BRE",
    "quimper": "FR-BRE",
    "brest": "FR-BRE",
    "lannion": "FR-BRE",
    "tregor": "FR-BRE",
    "vannes": "FR-BRE",
    "toulon": "FR-PAC",
    "tpm": "FR-PAC",
    "metropoletpm": "FR-PAC",
    "var": "FR-PAC",
    "grenoble": "FR-ARA",
    "metropolegrenoble": "FR-ARA",
    "isere": "FR-ARA",
    "lyon": "FR-ARA",
    "grandlyon": "FR-ARA",
    "saintetienne": "FR-ARA",
    "clermont": "FR-ARA",
    "auvergne": "FR-ARA",
    "rhonealpes": "FR-ARA",
    "lepuyenvelay": "FR-ARA",
    "puyenvelay": "FR-ARA",
    "chambery": "FR-ARA",
    "savoie": "FR-ARA",
    "annecy": "FR-ARA",
    "valence": "FR-ARA",
    "guadeloupe": "FR-971",
    "cg971": "FR-971",
    "971": "FR-971",
    "martinique": "FR-972",
    "972": "FR-972",
    "reunion": "FR-974",
    "974": "FR-974",
    "mayotte": "FR-YT",
    "976": "FR-YT",
    "guyane": "FR-GF",
    "corsica": "FR-20R",
    "corse": "FR-20R",
    "corsicaopendata": "FR-20R",
    "bastia": "FR-20R",
    "ajaccio": "FR-20R",
    "npdc": "FR-HDF",
    "nordpasdecalais": "FR-HDF",
    "pasdecalais": "FR-HDF",
    "lille": "FR-HDF",
    "lillemetropole": "FR-HDF",
    "dunkerque": "FR-HDF",
    "tourcoing": "FR-HDF",
    "roubaix": "FR-HDF",
    "lens": "FR-HDF",
    "arras": "FR-HDF",
    "amiens": "FR-HDF",
    "saintquentin": "FR-HDF",
    "compiegne": "FR-HDF",
    "beauvais": "FR-HDF",
    "charentemaritime": "FR-NAQ",
    "larochelle": "FR-NAQ",
    "aggrolarochelle": "FR-NAQ",
    "bordeaux": "FR-NAQ",
    "bordeauxmetropole": "FR-NAQ",
    "gironde": "FR-NAQ",
    "hautegaronne": "FR-NAQ",
    "toulouse": "FR-OCC",
    "montpellier": "FR-OCC",
    "herault": "FR-OCC",
    "wwwheraultdata": "FR-OCC",
    "nimes": "FR-OCC",
    "perpignan": "FR-OCC",
    "carcassonne": "FR-OCC",
    "albi": "FR-OCC",
    "agen": "FR-NAQ",
    "limoges": "FR-NAQ",
    "poitiers": "FR-NAQ",
    "grandpoitiers": "FR-NAQ",
    "niort": "FR-NAQ",
    "angouleme": "FR-NAQ",
    "lacharente": "FR-NAQ",
    "dordogne": "FR-NAQ",
    "paysbasque": "FR-NAQ",
    "paysbasquefr": "FR-NAQ",
    "bayonne": "FR-NAQ",
    "biarritz": "FR-NAQ",
    "pau": "FR-NAQ",
    "le64": "FR-NAQ",
    "data64": "FR-NAQ",
    "paris": "FR-IDF",
    "opendataparis": "FR-IDF",
    "grandparis": "FR-IDF",
    "metropolegrandparis": "FR-IDF",
    "iledefrance": "FR-IDF",
    "idf": "FR-IDF",
    "versailles": "FR-IDF",
    "evry": "FR-IDF",
    "courcouronnes": "FR-IDF",
    "meudon": "FR-IDF",
    "issy": "FR-IDF",
    "montrouge": "FR-IDF",
    "saintry": "FR-IDF",
    "garges": "FR-IDF",
    "sqy": "FR-IDF",
    "saintquentinenyvelines": "FR-IDF",
    "chelles": "FR-IDF",
    "pontault": "FR-IDF",
    "noisiel": "FR-IDF",
    "torcy": "FR-IDF",
    "emerainville": "FR-IDF",
    "brousurchantereine": "FR-IDF",
    "courtry": "FR-IDF",
    "nantes": "FR-PDL",
    "paysdelaloire": "FR-PDL",
    "loireatlantique": "FR-PDL",
    "angers": "FR-PDL",
    "leman": "FR-PDL",
    "laval": "FR-PDL",
    "lemans": "FR-PDL",
    "la rochelle": "FR-NAQ",
    "larochelle": "FR-NAQ",
    "tours": "FR-CVL",
    "toursmetropole": "FR-CVL",
    "orleans": "FR-CVL",
    "orleansmetropole": "FR-CVL",
    "blois": "FR-CVL",
    "bourges": "FR-CVL",
    "chartres": "FR-CVL",
    "indreetloire": "FR-CVL",
    "loiret": "FR-CVL",
    "centrevaldeloire": "FR-CVL",
    "cd37": "FR-CVL",
    "observatoire41": "FR-CVL",
    "departement41": "FR-CVL",
    "lamayenne": "FR-PDL",
    "mayenne": "FR-PDL",
    "sarthe": "FR-PDL",
    "datasarthe": "FR-PDL",
    "soissons": "FR-HDF",
    "grandsoissons": "FR-HDF",
    "datavillesoissons": "FR-HDF",
    "saintavold": "FR-GES",
    "moselle": "FR-GES",
    "strasbourg": "FR-GES",
    "mulhouse": "FR-GES",
    "mulhousealsace": "FR-GES",
    "alsace": "FR-GES",
    "cigalsace": "FR-GES",
    "belfort": "FR-BFC",
    "grandbelfort": "FR-BFC",
    "epinal": "FR-GES",
    "vosges": "FR-GES",
    "thionville": "FR-GES",
    "metz": "FR-GES",
    "chalons": "FR-GES",
    "reims": "FR-GES",
    "avignon": "FR-PAC",
    "cartesmairieavignon": "FR-PAC",
    "aixenprovence": "FR-PAC",
    "marseille": "FR-PAC",
    "nice": "FR-PAC",
    "cotedazur": "FR-PAC",
    "datasud": "FR-PAC",
    "scotdatasud": "FR-PAC",
    "pilat": "FR-ARA",
    "parcnaturelpilat": "FR-ARA",
    "cartoparcnaturelpilat": "FR-ARA",
    "drome": "FR-ARA",
    "ardeche": "FR-ARA",
    "ain": "FR-ARA",
    "departementain": "FR-ARA",
    "rouen": "FR-NOR",
    "normandie": "FR-NOR",
    "caen": "FR-NOR",
    "lehavre": "FR-NOR",
    "caux": "FR-NOR",
    "cauxseine": "FR-NOR",
    "eure": "FR-NOR",
    "eurelien": "FR-NOR",
    "saintmalo": "FR-BRE",
    "stmalo": "FR-BRE",
    "stmaloagglomeration": "FR-BRE",
    "abbeville": "FR-HDF",
    "saintlouis": "FR-974",
    "saintlouisreunion": "FR-974",
    "portovecchio": "FR-20R",
    "lacq": "FR-NAQ",
    "orthez": "FR-NAQ",
    "cclacqorthez": "FR-NAQ",
    "opendatacclacqorthez": "FR-NAQ",
    "aude": "FR-OCC",
    "opendataaudefr": "FR-OCC",
    "aude": "FR-OCC",
    "tertnum": "FR-BFC",
    "trouverternumbfc": "FR-BFC",
    "bfc": "FR-BFC",
    "ofgl": "FR-PAC",
    "dataofglfr": "FR-PAC",
    "ratp": "FR-IDF",
    "dataratp": "FR-IDF",
    "sicoval": "FR-OCC",
    "datasicovalfr": "FR-OCC",
    "seineouest": "FR-IDF",
    "dataseineouestfr": "FR-IDF",
    "amp": "FR-PAC",
    "ampmetropole": "FR-PAC",
    "aixmarseille": "FR-PAC",
    "provence": "FR-PAC",
    "epn": "FR-HDF",
    "epnagglop": "FR-HDF",
    "fossurmer": "FR-971",
    "guadeloupe": "FR-971",
    "regionguadeloupe": "FR-971",
    "gwada": "FR-NAQ",
    "gwadair": "FR-NAQ",
    "datagwadairopendataarcgiscom": "FR-NAQ",
    "hawamayotte": "FR-YT",
    "datahawamayotte": "FR-YT",
    "atmoreunion": "FR-NAQ",
    "airpl": "FR-OCC",
    "airbreizh": "FR-BRE",
    "centrevaldeloire": "FR-CVL",
    "argenteuil": "FR-IDF",
    "saintavold": "FR-GES",
    "saintlouisagglo": "FR-974",
    "saintlouisreunion": "FR-974",
    "gers": "FR-OCC",
    "villedavray": "FR-IDF",
    "hautegaronne": "FR-OCC",
    "datavizhautegaronne": "FR-OCC",
    "fossurmer": "FR-PAC",
    "garges": "FR-IDF",
    "risorangis": "FR-IDF",
    "marennes": "FR-NAQ",
    "oleron": "FR-NAQ",
    "marennesoleron": "FR-NAQ",
    "compiegne": "FR-HDF",
    "creil": "FR-HDF",
    "creilsudoise": "FR-HDF",
    "haulartois": "FR-HDF",
    "artois": "FR-HDF",
    "picardie": "FR-HDF",
    "artoispicardie": "FR-HDF",
    "apur": "FR-IDF",
    "pvm": "FR-IDF",
    "agglopvm": "FR-IDF",
    "plainevallee": "FR-HDF",
    "geocompiegne": "FR-HDF",
    "compiegnois": "FR-HDF",
    "pigma": "FR-OCC",
    "geo2france": "FR-IDF",
    "sportssg": "FR-IDF",
    "smiddest": "FR-GES",
    "dilleaubigne": "FR-NAQ",
    "datara": "FR-IDF",
    "cataloguedatara": "FR-IDF",
    "regioncentre": "FR-CVL",
    "laregion": "FR-OCC",
    "occitanie": "FR-OCC",
    "sigloire": "FR-ARA",
    "cataloguesigloire": "FR-ARA",
    "val2c": "FR-CVL",
    "valdecher": "FR-CVL",
    "controis": "FR-CVL",
    "datasmartidf": "FR-IDF",
    "cerema": "FR-IDF",
    "indores": "FR-CVL",
    "wwwindoresfr": "FR-CVL",
    "karugeo": "FR-971",
    "cataloguekarugeofr": "FR-971",
    "geomartinique": "FR-972",
    "cataloguegeomartiniquefr": "FR-972",
    "catalogueguyanesigfr": "FR-GF",
    "geopal": "FR-BRE",
    "cataloguegeopalorg": "FR-BRE",
    "dataragouv": "FR-IDF",
    "cataloguedataragouvfr": "FR-IDF",
    "parcnational": "FR-20R",
    "catalogueparcnationalfr": "FR-20R",
    "geosas": "FR-NAQ",
    "apigeosasfr": "FR-NAQ",
}


def tokenize(text: str) -> list[str]:
    t = (text or "").lower()
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return [x for x in t.split() if len(x) >= 2]


def infer_subregion(record: dict) -> tuple[str, str] | None:
    """Infer (subregion_id, subregion_name) from record content."""
    owner = record.get("owner") or {}
    owner_name = (owner.get("name") or "").strip()
    link = (record.get("link") or "").strip()
    rid = (record.get("id") or "").strip()
    desc = (record.get("description") or "").strip()
    combined = f"{owner_name} {link} {rid} {desc}".lower()

    # 1. Place names - full token match
    tokens = tokenize(combined)
    for tok in tokens:
        if tok in PLACE_TO_REGION:
            region = PLACE_TO_REGION[tok]
            return (region, REGION_NAMES.get(region, region))

    # 1b. Place names - substring match (e.g. loireatlantique in servicesvuducielloireatlantiquefr)
    combined_nospace = combined.replace(" ", "")
    for place, region in sorted(PLACE_TO_REGION.items(), key=lambda x: -len(x[0])):
        if len(place) >= 5 and place in combined_nospace:
            return (region, REGION_NAMES.get(region, region))

    # 2. Department number in id/link (e.g. geoplateforme17, opendata56, tourisme62)
    for m in re.finditer(r"(?:^|[\D])(\d{2})(?:[\D]|$)", combined.replace(" ", "")):
        dept = m.group(1)
        if dept in DEPT_TO_REGION:
            region = DEPT_TO_REGION[dept]
            return (region, REGION_NAMES.get(region, region))

    return None


def main(dry_run: bool = True) -> None:
    if not FR_UNK.exists():
        print("FR-UNK directory not found")
        return

    moved = 0
    skipped = 0

    for subdir in ("opendata", "geo", "scientific", "indicators", "microdata", "api", "metadata", "other"):
        src_dir = FR_UNK / subdir
        if not src_dir.exists():
            continue

        for yaml_path in sorted(src_dir.glob("*.yaml")):
            try:
                data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"  SKIP (parse error) {yaml_path.name}: {e}")
                skipped += 1
                continue

            if not isinstance(data, dict):
                skipped += 1
                continue

            result = infer_subregion(data)
            if not result:
                print(f"  UNK {subdir}/{yaml_path.name} (no match)")
                skipped += 1
                continue

            region_id, region_name = result
            dst_dir = ENTITIES_DIR / "FR" / region_id / subdir
            dst_path = dst_dir / yaml_path.name

            if dst_path.exists() and dst_path.resolve() != yaml_path.resolve():
                print(f"  SKIP (exists) {yaml_path.name} -> {region_id}/{subdir}/")
                skipped += 1
                continue

            # Update YAML subregion in owner.location and coverage
            changed = False
            for loc_key in ("owner",):
                obj = data.get(loc_key)
                if isinstance(obj, dict):
                    loc = obj.get("location") or {}
                    if isinstance(loc, dict):
                        sr = loc.get("subregion")
                        if not isinstance(sr, dict) or sr.get("id") != region_id:
                            if "location" not in obj:
                                obj["location"] = {}
                            obj["location"]["subregion"] = {"id": region_id, "name": region_name}
                            obj["location"]["level"] = 30
                            changed = True

            coverage = data.get("coverage") or []
            if isinstance(coverage, list) and coverage:
                entry = coverage[0]
                if isinstance(entry, dict):
                    loc = entry.get("location") or {}
                    if isinstance(loc, dict):
                        sr = loc.get("subregion")
                        if not isinstance(sr, dict) or sr.get("id") != region_id:
                            if "location" not in entry:
                                entry["location"] = {}
                            entry["location"]["subregion"] = {"id": region_id, "name": region_name}
                            changed = True

            if dry_run:
                print(f"  MOVE {subdir}/{yaml_path.name} -> FR/{region_id}/{subdir}/ ({region_name})")
            else:
                dst_dir.mkdir(parents=True, exist_ok=True)
                dst_path.write_text(
                    yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=False),
                    encoding="utf-8",
                )
                yaml_path.unlink()
                print(f"  OK {yaml_path.name} -> FR/{region_id}/{subdir}/")

            moved += 1

    print(f"\n{'Would move' if dry_run else 'Moved'}: {moved}, Skipped: {skipped}")

    if dry_run and moved > 0:
        print("\nRun with --apply to perform moves.")


if __name__ == "__main__":
    import sys
    main(dry_run="--apply" not in sys.argv)
