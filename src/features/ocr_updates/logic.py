# src/features/ocr_updates/logic.py
import logging
from datetime import datetime

from src.core.database import db_instance

from .pipeline.parser import parse_payload
from .pipeline.transfer import transfer_all_files
from .pipeline.validation import validate_metadata_strict
from .pipeline.context_naming import build_context_names
from .pipeline.builder import build_document_record
from .pipeline.repository import upsert_document
from .pipeline.edges import create_structural_edges

logger = logging.getLogger(__name__)


async def process_ocr_result(payload: dict):
    try:
        db = db_instance.get_db()

        parsed = parse_payload(payload)

        logger.debug(f"Parsed OCR payload: {parsed}")

        context_entity_id = parsed.context_values.get("id")
        context_entity_type = parsed.context_values.get("type")

        logger.info(
            f"🔄 Procesando documento {parsed.task_id}. "
            f"Contexto: {context_entity_type} ({context_entity_id})"
        )

        # 1) Transferencia OCR MinIO -> tu MinIO (rutas relativas)
        base_path = f"stage-validate/{parsed.user_snapshot['id']}/{parsed.task_id}"
        stored_paths = await transfer_all_files(parsed.presigned_source, base_path)

        # 2) Validación estricta OCR
        validated_metadata, integrity_warnings = await validate_metadata_strict(
            db=db,
            schema_id=parsed.schema_info.get("id"),
            ocr_data=parsed.ocr_extracted_list,
        )

        # 3) Status final
        has_invalid_fields = any(not item["is_valid"] for item in validated_metadata.values())
        status = "attention_required" if has_invalid_fields or integrity_warnings else "validated"

        # 4) Naming desde el grafo (DEVUELVE DICT)
        naming = build_context_names(db, context_entity_id)

        # 5) Construcción record final
        document_record = build_document_record(
            task_id=parsed.task_id,
            timestamp=parsed.timestamp,
            internal_result=parsed.internal_result,
            user_snapshot=parsed.user_snapshot,
            status=status,
            stored_paths=stored_paths,
            validated_metadata=validated_metadata,
            integrity_warnings=integrity_warnings,
            context_values=parsed.context_values,
            schema_info=parsed.schema_info,
            now_iso=datetime.now().isoformat(),
            naming=naming,
            required_document=parsed.required_document,  # <--- Pasamos el dato
        )

        # 6) Persistencia
        upsert_document(db, document_record)
        logger.info(f" Documento guardado. Estado: {status}")

        # 7) Edges estructurales
        await create_structural_edges(
            db,
            task_id=parsed.task_id,
            schema_id=parsed.schema_info.get("id"),
            context_entity_id=context_entity_id,
            context_entity_type=context_entity_type,
            required_doc_id=parsed.required_document.get("id"),
        )

    except Exception as e:
        logger.error(f"Error CRÍTICO en lógica OCR: {e}", exc_info=True)
