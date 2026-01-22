import logging
from typing import Any, Dict, List, Tuple
import re
from .user_lookup import lookup_user_in_microsoft_graph

logger = logging.getLogger(__name__)


async def validate_metadata_strict(db, schema_id: str, ocr_data: List[Dict[str, Any]]) -> Tuple[
    Dict[str, Any], List[str]]:
    validated_output = {}
    integrity_warnings = []

    # 1. Cargar esquema
    schema_definitions = {}
    if schema_id and db.has_collection("meta_schemas"):
        try:
            schema_doc = db.collection("meta_schemas").get(schema_id)
            if schema_doc:
                for field in schema_doc.get("fields", []):
                    schema_definitions[field["fieldKey"]] = field
        except Exception as e:
            logger.warning(f"⚠️ Error cargando esquema {schema_id}: {e}")

    # 2. Validar campos
    for item in ocr_data:
        key = item.get("fieldKey")
        raw_value = item.get("response")

        field_def = schema_definitions.get(key)
        if not field_def:
            continue

        label = field_def.get("label", key)

        if _is_entity_field(field_def):
            logger.info(f"🔍 Validando campo '{label}' (Key: {key}) con valor: '{raw_value}'")
            entity_type_key = field_def.get("entityType", {}).get("key")

            match = await _find_entity_match(db, raw_value, entity_type_key)

            # B) Fallback a Microsoft Graph (Solo usuarios)
            source_tag = "db_smart_match"
            if not match and entity_type_key in ["user", "person"]:
                logger.info(f"Fallo local para usuario. Intentando Microsoft Graph...")
                match = await lookup_user_in_microsoft_graph(db, str(raw_value))
                if match: source_tag = "microsoft_graph"

            if match:
                # CASO 1: Es un USUARIO / PERSONA (Estructura Rca)
                if entity_type_key in ["user", "person", "usuario"] or match.get("type") == "usuario":
                    logger.info(match)
                    value_data = {
                        "id": match["_key"],
                        "first_name": match.get("name"),  # Nombres separados
                        "last_name": match.get("last_name"),  # Apellidos separados
                        "email": match.get("mail") or match.get("email") or match.get("userPrincipalName")
                    }

                # CASO 2: Es una ENTIDAD ESTRUCTURAL (Facultad, Carrera)
                else:
                    value_data = {
                        "id": match["_key"],
                        "name": match.get("name"),
                        "code": match.get("code") or match.get("code_numeric"),
                        "type": match.get("type")
                    }

                validated_output[key] = {
                    "value": value_data,
                    "is_valid": True,
                    "source": source_tag
                }
                logger.info(f"🎯 MATCH CONFIRMADO [{label}]: '{raw_value}' -> '{match.get('name')}'")
            else:
                # --- AQUÍ FALTABA EL LOG ---
                logger.warning(
                    f"NO SE ENCONTRÓ MATCH para [{label}]. Valor OCR: '{raw_value}'. Tipo buscado: {entity_type_key}")

                validated_output[key] = {
                    "value": raw_value,
                    "is_valid": False,
                    "message": f"No se encontró {label} similar en el sistema.",
                    "source": "ocr_raw"
                }
                integrity_warnings.append(f"Campo '{label}' no coincide con registros institucionales.")
        else:
            validated_output[key] = {
                "value": raw_value,
                "is_valid": True,
                "source": "ocr_raw"
            }

    return validated_output, integrity_warnings


async def _find_entity_match(db, text_from_ocr: Any, entity_type_key: str | None):
    if not text_from_ocr:
        return None

    q_raw = str(text_from_ocr).strip().replace("\n", " ")
    q = re.sub(r"\s+", " ", q_raw)
    if len(q) < 3:
        return None

    type_map = {
        "career": "carrera",
        "faculty": "facultad",
        "department": "departamento",
        "user": "usuario",
        "person": "usuario",
    }
    db_type = type_map.get(entity_type_key, entity_type_key)

    name_analyzer = "text_es"
    type_analyzer = "norm_es"

    logger.info(f"   🔎 ArangoSearch q='{q}' type='{db_type}'")

    aql = f"""
    FOR doc IN entities_search_view
      SEARCH
        (
          // 1) match exacto por código
          doc.code == @q OR doc.code_numeric == @q

          // 2) phrase (más estricto)
          OR ANALYZER(PHRASE(doc.name, @q), "{name_analyzer}")

          // 3) token search (más flexible)
          OR ANALYZER(doc.name IN TOKENS(@q, "{name_analyzer}"), "{name_analyzer}")
        )
        AND (
          @db_type == null
          OR ANALYZER(doc.type == @db_type, "{type_analyzer}")
          OR ANALYZER(doc.type == @db_type, "identity")
        )
      LET score = BM25(doc)
      SORT score DESC
      LIMIT 5
      RETURN {{ doc: doc, score: score }}
    """

    try:
        rows = list(db.aql.execute(aql, bind_vars={"q": q, "db_type": (db_type or None)}))
        if not rows:
            logger.warning("      ⚠️ ArangoSearch devolvió 0 resultados.")
            return None

        for i, r in enumerate(rows):
            d = r["doc"]
            logger.info(f"      👉 Cand#{i+1}: {d.get('name')} type={d.get('type')} score={r['score']}")

        best = rows[0]
        # Ajusta umbral según tus datos; 0.15–0.30 suele ser más realista que 0.1
        return best["doc"] if best["score"] >= 0.15 else None

    except Exception as e:
        logger.warning(f"      ⚠️ Error ArangoSearch: {e}")
        return None

def _is_entity_field(field_def):
    has_type_id = field_def.get("entityTypeId") is not None
    type_input_key = field_def.get("typeInput", {}).get("key")
    return has_type_id or type_input_key in ["entity", "faculty", "career", "user", "person"]