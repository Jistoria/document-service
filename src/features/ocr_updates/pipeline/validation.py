# src/features/ocr_updates/pipeline/validation.py
import logging
import re
from typing import Any, Dict, List, Tuple
from .user_lookup import find_user_with_fallback

logger = logging.getLogger(__name__)

PERSON_FIELD_KEYS = {"author", "tutor"}  # agrega más si aplica: "reviewer", etc.

async def validate_metadata_strict(
    db,
    *,
    schema_id: str | None,
    ocr_data: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Valida metadata OCR contra el esquema (meta_schemas) en ArangoDB.
    - Campos tipo entidad: intenta match en colección 'entidades'
    - academic_period: regex 20XX-1/2
    - texto genérico: inválido si > 100 chars
    """
    validated_output: Dict[str, Any] = {}
    warnings: List[str] = []

    # 1) cargar definiciones de esquema
    schema_definitions: Dict[str, Any] = {}
    if schema_id and db.has_collection("meta_schemas"):
        schema_doc = db.collection("meta_schemas").get(schema_id)
        if schema_doc:
            for field in schema_doc.get("fields", []):
                schema_definitions[field["fieldKey"]] = field

    if not schema_definitions:
        warnings.append("No se encontró definición de esquema.")

    # 2) validar cada item OCR
    for item in ocr_data or []:
        key = item.get("fieldKey")
        raw_value = item.get("response")
        field_def = schema_definitions.get(key)

        if not field_def:
            continue

        label = field_def.get("label", key)

        # A) Validación de entidad (match en BD)
        if is_entity_field(field_def):
            entity_type = (field_def.get("entityType", {}) or {}).get("key")
            match = find_entity_match(db, raw_value, entity_type)

            if match:
                validated_output[key] = {
                    "value": {
                        "id": match["_key"],
                        "name": match.get("name"),
                        "code": match.get("code"),
                    },
                    "is_valid": True,
                    "source": "database_match",
                }
            else:
                validated_output[key] = {
                    "value": raw_value,
                    "is_valid": False,
                    "message": f"No se encontró {label} en el sistema.",
                    "source": "ocr_raw",
                }
                warnings.append(f"Campo '{label}' no coincide con registros.")

        # B) Validación Periodo Académico
        elif key == "academic_period":
            if raw_value and re.search(r"\b20\d{2}[-][1-2]\b", str(raw_value)):
                validated_output[key] = {
                    "value": raw_value,
                    "is_valid": True,
                    "source": "regex_match",
                }
            else:
                validated_output[key] = {
                    "value": raw_value,
                    "is_valid": False,
                    "message": "Formato inválido (Ej: 2025-1)",
                    "source": "ocr_raw",
                }

        elif key in PERSON_FIELD_KEYS:
            result = await find_user_with_fallback(db, str(raw_value or ""))
            if result:
                # Si viene de DB: guardamos _key interno
                if result["source"] in ("db", "graph_cached"):
                    u = result["user"]
                    validated_output[key] = {
                        "value": {
                            "id": u.get("_key"),
                            "name": f"{u.get('name', '')}".strip(),
                            "last_name": f"{u.get('last_name', '')}".strip(),
                            "email": u.get("email"),
                        },
                        "is_valid": True,
                        "source": "db_user_match",
                    }
                else:
                    # Viene de Graph: guardamos azure_id y datos base
                    u = result["user"]
                    validated_output[key] = {
                        "value": {
                            "azure_id": u.get("azure_id"),
                            "displayName": u.get("displayName"),
                            "mail": u.get("mail"),
                            "userPrincipalName": u.get("userPrincipalName"),
                            "jobTitle": u.get("jobTitle"),
                            "companyName": u.get("companyName"),
                            "officeLocation": u.get("officeLocation"),
                        },
                        "is_valid": True,
                        "source": "graph_user_match",
                    }
            else:
                validated_output[key] = {
                    "value": raw_value,
                    "is_valid": False,
                    "message": "No se encontró la persona en usuarios ni en Microsoft Graph.",
                    "source": "ocr_raw",
                }
                warnings.append(f"Campo '{label}' no coincide con usuarios.")

        # C) Texto genérico
        else:
            if raw_value and len(str(raw_value)) > 100:
                validated_output[key] = {
                    "value": raw_value,
                    "is_valid": False,
                    "message": "Texto demasiado largo.",
                    "source": "ocr_raw",
                }
            else:
                validated_output[key] = {
                    "value": raw_value,
                    "is_valid": True,
                    "source": "ocr_raw",
                }

    return validated_output, warnings


# ---------------- HELPERS ----------------

def is_entity_field(field_def: Dict[str, Any]) -> bool:
    has_type_id = field_def.get("entityTypeId") is not None
    type_input_key = (field_def.get("typeInput", {}) or {}).get("key")
    return bool(has_type_id or type_input_key in ["entity", "faculty", "career"])


def find_entity_match(db, text: Any, type_key: str | None):
    if not text or len(str(text).strip()) < 3:
        return None

    text_clean = str(text).strip()

    aql = """
    FOR e IN entidades
        FILTER e.type == @type_key
        FILTER CONTAINS(LOWER(e.name), LOWER(@search)) OR e.code == @search
        LIMIT 1
        RETURN e
    """
    try:
        cursor = db.aql.execute(
            aql,
            bind_vars={"type_key": type_key, "search": text_clean},
        )
        result = list(cursor)
        return result[0] if result else None
    except Exception as ex:
        logger.warning(f"Error buscando match entidad: {ex}")
        return None
