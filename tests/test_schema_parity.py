import json
import os
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

REPO_ROOT = Path(__file__).resolve().parent.parent
CERBERUS_SCHEMA_PATH = REPO_ROOT / "data" / "schemes" / "catalog.json"
JSON_SCHEMA_PATH = REPO_ROOT / "data" / "schemes" / "catalog.schema.json"
CONTEXT_PATH = REPO_ROOT / "data" / "schemes" / "catalog.context.jsonld"


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _cerberus_required_fields(schema: dict) -> list[str]:
    return sorted(
        field_name
        for field_name, rules in schema.items()
        if isinstance(rules, dict) and rules.get("required")
    )


def _cerberus_allowed_values(schema: dict, field_name: str) -> list[str] | None:
    rules = schema.get(field_name, {})
    allowed = rules.get("allowed")
    if allowed is None and rules.get("type") == "list":
        item_schema = rules.get("schema", {})
        allowed = item_schema.get("allowed")
    return sorted(allowed) if allowed is not None else None


def _json_schema_enum(schema: dict, field_name: str) -> list[str] | None:
    props = schema.get("properties", {})
    rules = props.get(field_name, {})
    if "enum" in rules:
        return sorted(rules["enum"])
    if rules.get("type") == "array":
        items = rules.get("items", {})
        if "enum" in items:
            return sorted(items["enum"])
    return None


def test_required_fields_match_between_cerberus_and_json_schema():
    cerberus = _load_json(CERBERUS_SCHEMA_PATH)
    json_schema = _load_json(JSON_SCHEMA_PATH)
    assert _cerberus_required_fields(cerberus) == sorted(json_schema.get("required", []))


@pytest.mark.parametrize(
    "field_name",
    ["catalog_type", "status", "access_mode"],
)
def test_enum_parity(field_name: str):
    cerberus = _load_json(CERBERUS_SCHEMA_PATH)
    json_schema = _load_json(JSON_SCHEMA_PATH)
    cerberus_values = _cerberus_allowed_values(cerberus, field_name)
    json_values = _json_schema_enum(json_schema, field_name)
    assert cerberus_values is not None
    assert json_values is not None
    assert cerberus_values == json_values


def test_json_schema_has_descriptions_for_top_level_properties():
    json_schema = _load_json(JSON_SCHEMA_PATH)
    for field_name, rules in json_schema["properties"].items():
        assert "description" in rules, f"missing description for {field_name}"
        assert rules["description"].strip()


def test_sample_catalog_validates_against_json_schema():
    jsonschema = pytest.importorskip("jsonschema")
    sample_path = REPO_ROOT / "data" / "entities" / "US" / "US-DC" / "opendata" / "catalogdatagov.yaml"
    with sample_path.open("r", encoding="utf-8") as handle:
        record = yaml.safe_load(handle)
    schema = _load_json(JSON_SCHEMA_PATH)
    jsonschema.Draft202012Validator(schema).validate(record)


def test_jsonld_context_maps_core_fields():
    context = _load_json(CONTEXT_PATH)
    mapped = context["@context"]
    assert mapped["name"] == "dct:title"
    assert mapped["description"] == "dct:description"
    assert mapped["link"]["@id"] == "dcat:landingPage"
    assert mapped["status"] == "cdi:status"
    assert mapped["catalog_type"] == "cdi:catalogType"


def test_jsonld_export_framing(tmp_path):
    from jsonld_export import export_catalogs_jsonld, load_context, record_to_jsonld

    sample = {
        "uid": "cdi00000001",
        "id": "example",
        "name": "Example Catalog",
        "link": "https://example.gov/data",
    }
    input_path = tmp_path / "catalogs.jsonl"
    output_path = tmp_path / "catalogs.jsonld"
    input_path.write_text(json.dumps(sample) + "\n", encoding="utf-8")

    count = export_catalogs_jsonld(
        input_path=input_path,
        output_path=output_path,
        context_path=CONTEXT_PATH,
    )
    assert count == 1
    exported = json.loads(output_path.read_text(encoding="utf-8").strip())
    assert exported["@type"] == "dcat:DataCatalog"
    assert exported["@id"] == "urn:cdi:catalog:cdi00000001"
    assert "dct:title" in exported["@context"].values() or exported["@context"]["name"] == "dct:title"

    framed = record_to_jsonld(sample, load_context())
    assert framed["@type"] == "dcat:DataCatalog"
