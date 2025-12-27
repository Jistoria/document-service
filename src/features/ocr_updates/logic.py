import logging
import re
from datetime import datetime
from src.core.database import db_instance

logger = logging.getLogger(__name__)


async def process_ocr_result(payload: dict):
    try:
        db = db_instance.get_db()

        # --- 1. Desempaquetado de Datos ---
        task_id = payload.get("task_id")
        user_id = payload.get("user_id")
        timestamp = payload.get("timestamp")

        doc_data = payload.get("document_data", {})
        internal_result = doc_data.get("internal_result", {})

        # Datos Crudos del OCR (Contenido variable/falible)
        ocr_extracted_list = internal_result.get("metadata", [])

        # Contexto Inmutable (La verdad absoluta del sistema)
        external_doc = doc_data.get("external_document", {})
        file_info = external_doc.get("files", [{}])[0]

        # IDs de Contexto
        context_values = file_info.get("metadataValues", {})
        context_entity_id = context_values.get("id")  # ID de la Carrera/Facultad
        context_entity_type = context_values.get("type")  # Ej: Carrera

        # IDs de Esquema
        schema_info = file_info.get("metadataSchema", {})
        schema_id = schema_info.get("id")

        logger.info(f"🔄 Procesando documento {task_id}. Contexto: {context_entity_type} ({context_entity_id})")

        # --- 2. VALIDACIÓN ESTRICTA DEL CONTENIDO (OCR) ---
        # Esto solo sirve para decirle al usuario "Oye, lo que leímos no coincide con lo que esperábamos"
        # pero NO afecta a dónde se guarda el documento.
        validated_metadata, integrity_warnings = await _validate_metadata_strict(
            db,
            schema_id,
            ocr_extracted_list
        )

        # --- 3. Determinar Estado ---
        has_invalid_fields = any(not item['is_valid'] for item in validated_metadata.values())
        status = "attention_required" if has_invalid_fields or integrity_warnings else "validated"

        # --- 4. Construir Documento ---
        document_record = {
            "_key": task_id,
            "user_id": user_id,
            "status": status,
            "original_filename": internal_result.get("filename"),
            "processing_time": internal_result.get("processing_time"),
            "created_at": timestamp,
            "updated_at": datetime.now().isoformat(),

            "storage": {
                "pdf_url": internal_result.get("presigned_urls", {}).get("minio_pdfa"),
                "json_validated_url": internal_result.get("presigned_urls", {}).get("minio_validated"),
                "text_url": internal_result.get("presigned_urls", {}).get("minio_text")
            },

            "validated_metadata": validated_metadata,
            "integrity_warnings": integrity_warnings,

            # Guardamos Snapshot del Contexto (Backup)
            "context_snapshot": {
                "entity_id": context_entity_id,
                "entity_name": context_values.get("name"),
                "schema_id": schema_id,
                "schema_name": schema_info.get("name")
            }
        }

        # --- 5. Guardar Vértice (Documento) ---
        if not db.has_collection("documents"):
            db.create_collection("documents")

        db.collection("documents").insert(document_record, overwrite=True)
        logger.info(f"✅ Documento guardado. Estado: {status}")

        # --- 6. RESTAURACIÓN DE EDGES (RELACIONES ESTRUCTURALES) ---
        # Estas relaciones son SAGRADAS, vienen del JSON externo, no del OCR.

        # A. Relación Documento -> Esquema (meta_schemas)
        if schema_id:
            await _create_safe_edge(
                db,
                from_id=f"documents/{task_id}",
                to_id=f"meta_schemas/{schema_id}",
                collection="usa_esquema",
                edge_key=f"{task_id}_{schema_id}"
            )

        # B. Relación Documento -> Entidad Organizativa (entidades)
        # Conecta el documento a la Carrera o Facultad donde se subió
        if context_entity_id:
            await _create_safe_edge(
                db,
                from_id=f"documents/{task_id}",
                to_id=f"entidades/{context_entity_id}",
                collection="pertenece_a",
                edge_key=f"{task_id}_{context_entity_id}"
            )
            logger.info(f"🔗 Relación creada: Documento -> {context_entity_type}")

    except Exception as e:
        logger.error(f"❌ Error CRÍTICO en lógica OCR: {e}", exc_info=True)


# --- LÓGICA DE VALIDACIÓN (Igual que antes, modo estricto) ---

async def _validate_metadata_strict(db, schema_id, ocr_data):
    validated_output = {}
    warnings = []

    schema_definitions = {}
    if schema_id and db.has_collection("meta_schemas"):
        schema_doc = db.collection("meta_schemas").get(schema_id)
        if schema_doc:
            for field in schema_doc.get("fields", []):
                schema_definitions[field["fieldKey"]] = field

    if not schema_definitions:
        warnings.append("No se encontró definición de esquema.")

    for item in ocr_data:
        key = item.get("fieldKey")
        raw_value = item.get("response")
        field_def = schema_definitions.get(key)

        if not field_def: continue

        label = field_def.get("label", key)

        # Validación Entidades (Busca match en BD, si falla es inválido)
        if _is_entity_field(field_def):
            entity_type = field_def.get("entityType", {}).get("key")
            match = _find_entity_match(db, raw_value, entity_type)

            if match:
                validated_output[key] = {
                    "value": {"id": match["_key"], "name": match["name"], "code": match.get("code")},
                    "is_valid": True,
                    "source": "database_match"
                }
            else:
                validated_output[key] = {
                    "value": raw_value,
                    "is_valid": False,  # <--- MARCA ERROR SI NO HAY MATCH
                    "message": f"No se encontró {label} en el sistema.",
                    "source": "ocr_raw"
                }
                warnings.append(f"Campo '{label}' no coincide con registros.")

        # Validación Periodo Académico
        elif key == "academic_period":
            if raw_value and re.search(r'\b20\d{2}[-][1-2]\b', raw_value):
                validated_output[key] = {"value": raw_value, "is_valid": True, "source": "regex_match"}
            else:
                validated_output[key] = {
                    "value": raw_value,
                    "is_valid": False,
                    "message": "Formato inválido (Ej: 2025-1)",
                    "source": "ocr_raw"
                }

        # Texto Genérico
        else:
            if raw_value and len(raw_value) > 100:
                validated_output[key] = {"value": raw_value, "is_valid": False, "message": "Texto demasiado largo.",
                                         "source": "ocr_raw"}
            else:
                validated_output[key] = {"value": raw_value, "is_valid": True, "source": "ocr_raw"}

    return validated_output, warnings


# --- HELPERS ---

def _is_entity_field(field_def):
    has_type_id = field_def.get("entityTypeId") is not None
    type_input_key = field_def.get("typeInput", {}).get("key")
    return has_type_id or type_input_key in ["entity", "faculty", "career"]


def _find_entity_match(db, text, type_key):
    if not text or len(text) < 3: return None
    text_clean = text.strip()
    aql = """
    FOR e IN entidades
        FILTER e.type == @type_key
        FILTER CONTAINS(LOWER(e.name), LOWER(@search)) OR e.code == @search
        LIMIT 1
        RETURN e
    """
    try:
        cursor = db.aql.execute(aql, bind_vars={"type_key": type_key, "search": text_clean})
        result = list(cursor)
        return result[0] if result else None
    except Exception:
        return None


async def _create_safe_edge(db, from_id, to_id, collection, edge_key):
    """Crea una arista de forma segura usando AQL"""
    if not db.has_collection(collection):
        db.create_collection(collection, edge=True)

    aql = f"""
    UPSERT {{ _key: @key }}
    INSERT {{ _key: @key, _from: @from_id, _to: @to_id, created_at: DATE_NOW() }}
    UPDATE {{ updated_at: DATE_NOW() }}
    IN {collection}
    """
    db.aql.execute(aql, bind_vars={"key": edge_key, "from_id": from_id, "to_id": to_id})