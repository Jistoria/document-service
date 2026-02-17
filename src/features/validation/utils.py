import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


USER_ENTITY_TYPES = {"user", "person", "usuario"}


def is_user_type(entity_type: Optional[str]) -> bool:
    return (entity_type or "").lower() in USER_ENTITY_TYPES


def looks_like_user_payload(value: Dict[str, Any]) -> bool:
    return any(key in value for key in ("first_name", "last_name", "display_name", "email"))


def map_type_to_collection(entity_type: Optional[str]) -> Optional[str]:
    mapping = {
        "user": "dms_users",
        "person": "dms_users",
        "faculty": "entities",
        "career": "entities",
        "department": "entities",
        "facultad": "entities",
        "carrera": "entities",
    }
    return mapping.get((entity_type or "").lower())


def get_schema_for_document(db, doc_id: str) -> Optional[Dict[str, Any]]:
    aql = """
    FOR doc IN documents
        FILTER doc._key == @doc_id
        FOR schema IN 1..1 OUTBOUND doc usa_esquema
        RETURN schema
    """
    cursor = db.aql.execute(aql, bind_vars={"doc_id": doc_id})
    schemas = list(cursor)
    return schemas[0] if schemas else None


def allowed_keys_from_schema(schema: Dict[str, Any]) -> set[str]:
    return {f.get("fieldKey") for f in (schema.get("fields") or []) if f.get("fieldKey")}


def entity_types_from_schema(schema: Dict[str, Any]) -> Dict[str, Optional[str]]:
    return {
        field.get("fieldKey"): (field.get("entityType") or {}).get("key")
        for field in (schema.get("fields") or [])
        if field.get("fieldKey")
    }


def filter_entity_fields(val: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deja SOLO lo útil de dominio (nada de is_valid/source/message/etc).
    """
    if looks_like_user_payload(val):
        display_name = val.get("display_name")
        if not display_name:
            fn = (val.get("first_name") or "").strip()
            ln = (val.get("last_name") or "").strip()
            display_name = f"{fn} {ln}".strip() or None

        out = {
            "id": val.get("id"),
            "display_name": display_name,
            "email": val.get("email"),
        }
        return {k: v for k, v in out.items() if v is not None}

    out = {
        "id": val.get("id"),
        "name": val.get("name"),
        "code": val.get("code"),
        "type": val.get("type"),
    }
    return {k: v for k, v in out.items() if v is not None}


def extract_metadata_value(value: Any) -> Any:
    """
    Devuelve el valor normalizado para búsqueda:
    - user/entity dict -> display_name/name/code/email (prioridad)
    - otros dict -> primer campo representativo
    - escalar -> mismo valor
    """
    if not isinstance(value, dict):
        return value

    for key in ("display_name", "name", "code", "email", "id"):
        candidate = value.get(key)
        if candidate not in (None, ""):
            return candidate

    for candidate in value.values():
        if candidate not in (None, ""):
            return candidate

    return None


def sanitize_metadata(raw_data: Dict[str, Any], allowed_keys: Optional[set[str]] = None) -> Dict[str, Any]:
    """
    Normaliza y limpia la metadata:
    - Elimina wrapper UI (is_valid/source/message/etc).
    - Para entidades deja solo campos permitidos.
    - Si item.is_valid == False => guarda None.
    - Si allowed_keys viene, SOLO guarda keys presentes en el schema.
    """
    clean: Dict[str, Any] = {}

    for key, item in (raw_data or {}).items():
        if allowed_keys is not None and key not in allowed_keys:
            continue

        if not isinstance(item, dict):
            clean[key] = {"value": item}
            continue

        if "value" in item:
            if item.get("is_valid") is False:
                clean[key] = None
                continue

            val = item.get("value")

            if isinstance(val, dict):
                normalized = filter_entity_fields(val)
                normalized["value"] = extract_metadata_value(normalized)
                clean[key] = normalized
                continue

            if val is None:
                minimal = {
                    "id": item.get("id"),
                    "display_name": item.get("display_name"),
                    "email": item.get("email"),
                }
                minimal = {k: v for k, v in minimal.items() if v}
                if minimal:
                    minimal["value"] = extract_metadata_value(minimal)
                    clean[key] = minimal
                else:
                    clean[key] = None
                continue

            clean[key] = {"value": val}
            continue

        normalized_item = dict(item)
        normalized_item["value"] = extract_metadata_value(normalized_item)
        clean[key] = normalized_item

    return clean


def build_display_name(name: Optional[str], last_name: Optional[str]) -> Optional[str]:
    display = " ".join(part for part in (name, last_name) if part)
    return display.strip() or None
