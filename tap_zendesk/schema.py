"""Schema loading for the JSON schemas shipped under ``tap_zendesk/schemas``."""

from __future__ import annotations

import json
from pathlib import Path

import singer

SCHEMAS_DIR = Path(__file__).parent / "schemas"
SHARED_SUBDIR = "shared"


def load_shared_schema_refs() -> dict:
    """Build the ref lookup for the cross-file ``shared/*.json`` schemas.

    The SDK does not resolve ``$ref`` on its own, and these schemas reference
    each other across files (e.g. ``{"$ref": "shared/attachments.json"}``), so
    the refs are keyed the same way the pre-SDK tap keyed them.

    Returns:
        A mapping of ``shared/<filename>`` to the parsed schema.
    """
    shared_dir = SCHEMAS_DIR / SHARED_SUBDIR
    return {
        f"{SHARED_SUBDIR}/{path.name}": json.loads(path.read_text())
        for path in sorted(shared_dir.iterdir())
        if path.is_file()
    }


def load_schema(name: str) -> dict:
    """Load a stream's JSON schema with all ``$ref`` entries resolved.

    Args:
        name: The stream name, matching the schema filename.

    Returns:
        The resolved JSON schema.
    """
    schema = json.loads((SCHEMAS_DIR / f"{name}.json").read_text())
    return singer.resolve_schema_references(schema, load_shared_schema_refs())


# Zendesk custom-field types, mapped to JSON schema types exactly as the
# pre-SDK tap mapped them.
CUSTOM_TYPES = {
    "text": "string",
    "textarea": "string",
    "date": "string",
    "regexp": "string",
    "dropdown": "string",
    "integer": "integer",
    "decimal": "number",
    "checkbox": "boolean",
    "lookup": "string",
    # Not in the pre-SDK tap's map, which raised on it. A multiselect holds
    # several option values, so it is an array of the option strings.
    "multiselect": "array",
}


def process_custom_field(field: dict) -> dict:
    """Return the JSON schema for one Zendesk custom field.

    Args:
        field: A custom field as returned by the API.

    Returns:
        The JSON schema for that field.

    Raises:
        ValueError: If the field's Zendesk type has no JSON schema equivalent.
    """
    zendesk_type = field.get("type")
    json_type = CUSTOM_TYPES.get(zendesk_type)
    if json_type is None:
        msg = (
            f"Discovered unsupported type for custom field {field.get('title')} "
            f"(key: {field.get('key')}): {zendesk_type}"
        )
        raise ValueError(msg)

    options = [o["value"] for o in field.get("custom_field_options") or []]

    if zendesk_type == "multiselect":
        return {"type": ["array", "null"], "items": {"type": "string", "enum": options}}

    field_schema: dict = {"type": [json_type, "null"]}
    if zendesk_type == "date":
        field_schema["format"] = "datetime"
    if zendesk_type == "dropdown":
        field_schema["enum"] = options
    return field_schema
