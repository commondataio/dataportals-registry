#!/usr/bin/env python3
"""
Fix ES-UNK records: infer real subregions and move to proper directories.

Records in data/entities/ES/ES-UNK with owner subregion ES-UNK are reviewed.
Real subregion is inferred from link domain, record id, owner name, and description.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

import yaml

BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
ES_UNK_DIR = ENTITIES_DIR / "ES" / "ES-UNK"

# Spanish subregion id -> display name (ISO 3166-2)
ES_SUBREGION_NAMES = {
    "ES-AN": "Andalucía",
    "ES-AR": "Aragón",
    "ES-AS": "Asturias",
    "ES-CB": "Cantabria",
    "ES-CE": "Ceuta",
    "ES-CL": "Castilla y León",
    "ES-CM": "Castilla-La Mancha",
    "ES-CN": "Canarias",
    "ES-CT": "Catalunya",
    "ES-EX": "Extremadura",
    "ES-GA": "Galicia",
    "ES-IB": "Illes Balears",
    "ES-MD": "Madrid, Comunidad de",
    "ES-MC": "Murcia, Región de",
    "ES-ML": "Melilla",
    "ES-NC": "Navarra, Comunidad Foral de",
    "ES-PV": "País Vasco",
    "ES-RI": "La Rioja",
    "ES-VC": "Valenciana, Comunidad",
}

# Token (lowercase) -> subregion_id. Longer/more specific tokens first when iterating.
# Order matters: we check longest match first.
TOKEN_TO_SUBREGION: list[tuple[str, str]] = [
    # Catalunya (ES-CT) - .cat domain, Barcelona area, Girona, Tarragona, Lleida
    ("barcelonacat", "ES-CT"),
    ("barcelona", "ES-CT"),
    ("dibacat", "ES-CT"),
    ("diba", "ES-CT"),
    ("gencat", "ES-CT"),
    ("ajuntamentbarcelona", "ES-CT"),
    ("sabadell", "ES-CT"),
    ("hospitalet", "ES-CT"),
    ("badalona", "ES-CT"),
    ("terrassa", "ES-CT"),
    ("vilanovacat", "ES-CT"),
    ("vilafranca", "ES-CT"),
    ("manlleu", "ES-CT"),
    ("esplugues", "ES-CT"),
    ("elprat", "ES-CT"),
    ("granollers", "ES-CT"),
    ("rubi", "ES-CT"),
    ("castelldefels", "ES-CT"),
    ("elvendrell", "ES-CT"),
    ("esparreguera", "ES-CT"),
    ("ripoll", "ES-CT"),
    ("olot", "ES-CT"),
    ("gava", "ES-CT"),
    ("paeria", "ES-CT"),
    ("viladecans", "ES-CT"),
    ("igualada", "ES-CT"),
    ("manresa", "ES-CT"),
    ("tarragona", "ES-CT"),
    ("figueres", "ES-CT"),
    ("amposta", "ES-CT"),
    ("girona", "ES-CT"),
    ("vallbona", "ES-CT"),
    ("lapobladevallbona", "ES-CT"),
    ("dadesobertes", "ES-CT"),
    ("seuecat", "ES-CT"),
    ("administraciodigitalgencat", "ES-CT"),
    ("opendatasantboi", "ES-CT"),
    ("santboi", "ES-CT"),
    # País Vasco (ES-PV)
    ("bilbao", "ES-PV"),
    ("donostia", "ES-PV"),
    ("gipuzkoa", "ES-PV"),
    ("bizkaia", "ES-PV"),
    ("vitoriagasteiz", "ES-PV"),
    ("vitoria", "ES-PV"),
    ("getxo", "ES-PV"),
    ("azkoitia", "ES-PV"),
    ("azpeitia", "ES-PV"),
    ("eibar", "ES-PV"),
    ("zarautz", "ES-PV"),
    ("leioa", "ES-PV"),
    ("airekiaeuses", "ES-PV"),
    ("opendatabizkaia", "ES-PV"),
    # Andalucía (ES-AN)
    ("malaga", "ES-AN"),
    ("sevilla", "ES-AN"),
    ("cordoba", "ES-AN"),
    ("cadiz", "ES-AN"),
    ("huelva", "ES-AN"),
    ("fuengirola", "ES-AN"),
    ("alhaurin", "ES-AN"),
    ("lucena", "ES-AN"),
    ("priego", "ES-AN"),
    ("montoro", "ES-AN"),
    ("pozoblanco", "ES-AN"),
    ("alcazar", "ES-AN"),
    ("losbarrios", "ES-AN"),
    ("costadelsol", "ES-AN"),
    ("sigurbanismosevilla", "ES-AN"),
    ("dipucadiz", "ES-AN"),
    ("dipucadizes", "ES-AN"),
    ("aljaraque", "ES-AN"),
    ("conil", "ES-AN"),
    ("turismoconil", "ES-AN"),
    ("aceituna", "ES-AN"),
    ("lagarganta", "ES-AN"),
    ("guijodesantabarbara", "ES-AN"),
    ("almendralejo", "ES-AN"),
    ("huelga", "ES-AN"),
    ("huelaga", "ES-AN"),
    ("alcazardesanjuan", "ES-AN"),
    ("villanuevadelduque", "ES-AN"),
    ("dipbadajoz", "ES-AN"),
    # Madrid (ES-MD)
    ("arganda", "ES-MD"),
    ("pinto", "ES-MD"),
    ("lasrozas", "ES-MD"),
    ("alcala", "ES-MD"),
    ("fuenlabrada", "ES-MD"),
    ("leganes", "ES-MD"),
    ("arroyomolinos", "ES-MD"),
    ("aytoarganda", "ES-MD"),
    ("alcorcon", "ES-MD"),
    ("villaviciosa", "ES-MD"),
    ("villaviciosadeodones", "ES-MD"),
    # Extremadura (ES-EX)
    ("caceres", "ES-EX"),
    ("badajoz", "ES-EX"),
    ("plasencia", "ES-EX"),
    ("trujillo", "ES-EX"),
    ("villanuevadelaserena", "ES-EX"),
    ("opendataaytocaceres", "ES-EX"),
    ("olivadeplasencia", "ES-EX"),
    ("portezuelo", "ES-EX"),
    ("robledillodetrujillo", "ES-EX"),
    ("saucedilla", "ES-EX"),
    ("santiagodelcampo", "ES-EX"),
    ("robledollano", "ES-EX"),
    ("sierradefuentes", "ES-EX"),
    ("montanchez", "ES-EX"),
    ("carcaboso", "ES-EX"),
    ("berrocalejo", "ES-EX"),
    ("palomero", "ES-EX"),
    ("villanuevadelasierra", "ES-EX"),
    ("villardelpedroso", "ES-EX"),
    ("monroy", "ES-EX"),
    ("benquerencia", "ES-EX"),
    ("guijodegalisteo", "ES-EX"),
    ("herguijuela", "ES-EX"),
    ("carbajo", "ES-EX"),
    ("riolobos", "ES-EX"),
    ("berzocana", "ES-EX"),
    ("talayuela", "ES-EX"),
    ("logrosan", "ES-EX"),
    ("acebo", "ES-EX"),
    ("descargamaria", "ES-EX"),
    ("higuera", "ES-EX"),
    ("acehuche", "ES-EX"),
    ("cachorrilla", "ES-EX"),
    ("madronera", "ES-EX"),
    ("navalvillardeibor", "ES-EX"),
    ("ahigal", "ES-EX"),
    ("casaresdelashurdes", "ES-EX"),
    ("membrio", "ES-EX"),
    ("talavan", "ES-EX"),
    ("viandardelavera", "ES-EX"),
    ("villanuevadelavera", "ES-EX"),
    ("ladrillar", "ES-EX"),
    ("belvisdemonroy", "ES-EX"),
    ("rosalejo", "ES-EX"),
    ("botija", "ES-EX"),
    ("deleitosa", "ES-EX"),
    ("valdefuentes", "ES-EX"),
    ("torreorgaz", "ES-EX"),
    ("valdemorales", "ES-EX"),
    ("eljas", "ES-EX"),
    ("hernanperez", "ES-EX"),
    ("santacruzdelasierra", "ES-EX"),
    ("valdecanas", "ES-EX"),
    ("aldeanuevadelcamino", "ES-EX"),
    ("pozuelodezarzon", "ES-EX"),
    ("aldeadelcano", "ES-EX"),
    ("caminomorisco", "ES-EX"),
    ("torrecilladelosangeles", "ES-EX"),
    ("barrado", "ES-EX"),
    ("ceclavin", "ES-EX"),
    ("jaraicejo", "ES-EX"),
    ("santibanezelbajo", "ES-EX"),
    ("casasdelmonte", "ES-EX"),
    ("villadelrey", "ES-EX"),
    ("navezuelas", "ES-EX"),
    ("plasenzuela", "ES-EX"),
    ("carrascalejo", "ES-EX"),
    ("alcuescar", "ES-EX"),
    ("torremocha", "ES-EX"),
    ("zorita", "ES-EX"),
    ("abertura", "ES-EX"),
    ("idepodepo", "ES-EX"),
    ("idepode", "ES-EX"),
    ("idecyl", "ES-CL"),
    ("idecyljcyl", "ES-CL"),
    # Castilla y León (ES-CL)
    ("valladolid", "ES-CL"),
    ("salamanca", "ES-CL"),
    ("burgos", "ES-CL"),
    ("ponferrada", "ES-CL"),
    ("valledemena", "ES-CL"),
    ("cuenca", "ES-CM"),
    ("dipucuenca", "ES-CM"),
    ("gobiernoabiertocuenca", "ES-CM"),
    # Castilla-La Mancha (ES-CM)
    ("laroda", "ES-CM"),
    ("albacete", "ES-CM"),
    ("aytolaroda", "ES-CM"),
    # Cantabria (ES-CB)
    ("camargo", "ES-CB"),
    ("santander", "ES-CB"),
    ("datosaytocamargo", "ES-CB"),
    ("datossantander", "ES-CB"),
    # Asturias (ES-AS)
    ("gijon", "ES-AS"),
    ("langreo", "ES-AS"),
    ("datosgijones", "ES-AS"),
    ("langreoasparticipalangreo", "ES-AS"),
    # Galicia (ES-GA)
    ("vigo", "ES-GA"),
    ("coruna", "ES-GA"),
    ("smartcoruna", "ES-GA"),
    ("datosckanvigo", "ES-GA"),
    # Canarias (ES-CN)
    ("tenerife", "ES-CN"),
    ("lapalma", "ES-CN"),
    ("canarias", "ES-CN"),
    ("santacruz", "ES-CN"),
    ("maspalomas", "ES-CN"),
    ("laspalmas", "ES-CN"),
    ("arona", "ES-CN"),
    ("puertodesantacruz", "ES-CN"),
    ("datostenerife", "ES-CN"),
    ("opendatalapalma", "ES-CN"),
    ("datoscanarias", "ES-CN"),
    ("eadminmaspalomas", "ES-CN"),
    ("eadminaridane", "ES-CN"),
    ("sedeteguise", "ES-CN"),
    ("datosarona", "ES-CN"),
    ("datosabiertoslaspalmas", "ES-CN"),
    # Illes Balears (ES-IB)
    ("ivissa", "ES-IB"),
    ("ibiza", "ES-IB"),
    ("conselldeivissa", "ES-IB"),
    ("opendataconselldeivissa", "ES-IB"),
    ("dadesobertesconselldeivissa", "ES-IB"),
    ("calpe", "ES-VC"),
    ("geoportalcalpe", "ES-VC"),
    # Valencia (ES-VC)
    ("alicante", "ES-VC"),
    ("valencia", "ES-VC"),
    ("sagunto", "ES-VC"),
    ("torrent", "ES-VC"),
    ("diputacionalicante", "ES-VC"),
    ("datosabiertossagunto", "ES-VC"),
    ("datosabiertostorrentes", "ES-VC"),
    # Aragón (ES-AR)
    ("zaragoza", "ES-AR"),
    ("teruel", "ES-AR"),
    ("zaragozaesciudadrisp", "ES-AR"),
    ("dpteruel", "ES-AR"),
    # Navarra (ES-NC)
    ("pamplona", "ES-NC"),
    ("pamplonaesayuntamiento", "ES-NC"),
    # Murcia (ES-MC)
    ("lorca", "ES-MC"),
    ("carm", "ES-MC"),
    ("opengeogiscarm", "ES-MC"),
    ("datoslorca", "ES-MC"),
    # Melilla (ES-ML)
    ("melilla", "ES-ML"),
    ("melilladatosabiertos", "ES-ML"),
    # Valencia - Alzira
    ("alzira", "ES-VC"),
    ("alziragvsigonline", "ES-VC"),
    # IDE regional - CHJ Júcar (Valencia), ARMA (Aragón), CIM (Madrid)
    ("idejucarchj", "ES-VC"),
    ("idearmimida", "ES-AR"),
    ("idecime", "ES-MD"),
    # Federal / nationwide
    ("agriculturaypesca", "Federal"),
    ("wwwgeoportalagriculturaypesca", "Federal"),
    ("ipcadmon", "ES-MD"),
    ("ipca", "ES-MD"),
    # More Extremadura municipalities (Cáceres)
    ("abadia", "ES-EX"),
    ("alagondelrio", "ES-EX"),
    ("aliseda", "ES-EX"),
    ("almoharin", "ES-EX"),
    ("aytoalia", "ES-EX"),
    ("banosdemontemayor", "ES-EX"),
    ("casasdemiravete", "ES-EX"),
    ("casillasdecoria", "ES-EX"),
    ("cuacosdeyuste", "ES-EX"),
    ("escurial", "ES-EX"),
    ("garciaz", "ES-EX"),
    ("gargantalaolla", "ES-EX"),
    ("herreradealcantara", "ES-EX"),
    ("hervas", "ES-EX"),
    ("hoyos", "ES-EX"),
    ("ibahernando", "ES-EX"),
    ("jarandilladelavera", "ES-EX"),
    ("jerte", "ES-EX"),
    ("lacumbre", "ES-EX"),
    ("majadas", "ES-EX"),
    ("mirabel", "ES-EX"),
    ("mohedasdegranadilla", "ES-EX"),
    ("navasdelmadron", "ES-EX"),
    ("robledillodelavera", "ES-EX"),
    ("salorino", "ES-EX"),
    ("sanmartindetrevejo", "ES-EX"),
    ("seguradetoro", "ES-EX"),
    ("toriles", "ES-EX"),
    ("torrejoncillo", "ES-EX"),
    ("valdeobispo", "ES-EX"),
    ("valverdedelavera", "ES-EX"),
    ("villadelcampo", "ES-EX"),
    # La Granja - Segovia (Castilla y León)
    ("lagranja", "ES-CL"),
    # Illes Balears
    ("intranetcaib", "ES-IB"),
    ("caib", "ES-IB"),
    # Andalucía - Cádiz
    ("dipcases", "ES-AN"),
    ("dipcadi", "ES-AN"),
    ("elpuertodesantamaria", "ES-AN"),
    ("alcobendas", "ES-MD"),
    ("montilla", "ES-AN"),
    ("eprinsa", "ES-AN"),
]

# Sort by token length descending so longer matches take precedence
TOKEN_TO_SUBREGION.sort(key=lambda x: -len(x[0]))


def extract_tokens(record: dict, file_path: str) -> str:
    """Extract searchable text from record (id, name, link, owner, description)."""
    parts = []
    parts.append(record.get("id") or "")
    parts.append(record.get("name") or "")
    parts.append(record.get("link") or "")
    parts.append(file_path)
    owner = record.get("owner") or {}
    parts.append(owner.get("name") or "")
    parts.append(owner.get("link") or "")
    parts.append(record.get("description") or "")
    text = " ".join(str(p) for p in parts).lower()
    # Normalize: remove punctuation, collapse spaces
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def infer_subregion(record: dict, file_path: str, catalog_type: str) -> str | None:
    """Infer subregion from record. Returns subregion_id or None."""
    text = extract_tokens(record, file_path)

    # Check .cat domain -> Catalunya
    link = (record.get("link") or "").lower()
    if ".cat" in link or "barcelona.cat" in link or "gencat.cat" in link:
        return "ES-CT"

    # Check .eus domain -> País Vasco
    if ".eus" in link or "bilbao.eus" in link or "donostia.eus" in link:
        return "ES-PV"

    # Token-based matching
    for token, subregion_id in TOKEN_TO_SUBREGION:
        if token in text:
            if subregion_id == "Federal":
                return "Federal"  # Move to ES/Federal
            return subregion_id

    return None


def main() -> None:
    import sys

    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN - no files will be moved\n")

    to_move: list[tuple[Path, str, str, dict]] = []  # (src, subregion, catalog_type, data)
    no_match: list[Path] = []

    for yaml_path in sorted(ES_UNK_DIR.rglob("*.yaml")):
        try:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  Skip {yaml_path.name}: {e}")
            continue
        if not isinstance(data, dict):
            continue

        rel_parts = yaml_path.relative_to(ES_UNK_DIR).parts
        catalog_type = rel_parts[0] if rel_parts else "opendata"  # geo, opendata, etc.
        file_path_str = str(yaml_path.relative_to(ENTITIES_DIR))

        subregion_id = infer_subregion(data, file_path_str, catalog_type)
        if subregion_id:
            to_move.append((yaml_path, subregion_id, catalog_type, data))
        else:
            no_match.append(yaml_path)

    print(f"Found {len(to_move)} records to move, {len(no_match)} with no match:\n")
    by_target: dict[str, list[tuple[Path, dict]]] = {}
    for path, subregion, _, record in to_move:
        key = f"ES/{subregion}" if subregion != "Federal" else "ES/Federal"
        by_target.setdefault(key, []).append((path, record))

    for target in sorted(by_target.keys()):
        items = by_target[target]
        print(f"  {target} ({len(items)}):")
        for path, record in items[:8]:
            print(f"    - {path.name}")
        if len(items) > 8:
            print(f"    ... and {len(items) - 8} more")
        print()

    if no_match:
        print(f"No match ({len(no_match)}):")
        for p in no_match[:15]:
            print(f"  - {p.name}")
        if len(no_match) > 15:
            print(f"  ... and {len(no_match) - 15} more")
        print()

    if dry_run or not to_move:
        return

    for src_path, subregion_id, catalog_type, data in to_move:
        if subregion_id == "Federal":
            target_dir = ENTITIES_DIR / "ES" / "Federal" / catalog_type
        else:
            target_dir = ENTITIES_DIR / "ES" / subregion_id / catalog_type
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / src_path.name

        # Update owner subregion in data (skip for Federal)
        owner = data.get("owner", {}) or {}
        loc = owner.get("location", {}) or {}
        if subregion_id != "Federal":
            loc["subregion"] = {
                "id": subregion_id,
                "name": ES_SUBREGION_NAMES.get(subregion_id, subregion_id),
            }
        else:
            loc.pop("subregion", None)
            if loc.get("level") == 30:
                loc["level"] = 20
        owner["location"] = loc
        data["owner"] = owner

        # Update coverage subregion if present (skip for Federal)
        if subregion_id != "Federal":
            coverage = data.get("coverage", [])
            if isinstance(coverage, list):
                for entry in coverage:
                    if isinstance(entry, dict):
                        loc_cov = entry.get("location", {}) or {}
                        if loc_cov.get("country", {}).get("id") == "ES":
                            loc_cov["subregion"] = {
                                "id": subregion_id,
                                "name": ES_SUBREGION_NAMES.get(subregion_id, subregion_id),
                            }
                            entry["location"] = loc_cov

        target_path.write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        src_path.unlink()
        dst_label = f"ES/Federal/{catalog_type}" if subregion_id == "Federal" else f"ES/{subregion_id}/{catalog_type}"
        print(f"Moved {src_path.name} -> {dst_label}/")

    print(f"\nMoved {len(to_move)} files. Run: python scripts/builder.py validate-yaml")


if __name__ == "__main__":
    main()
