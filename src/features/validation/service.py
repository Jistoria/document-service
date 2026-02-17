import logging
from datetime import datetime
import re
from typing import Optional

from src.core.database import db_instance
from src.features.validation.models import ValidationConfirmRequest, ValidationRequest

from .archive_service import ArchiveService, archive_service
from .entities_service import EntitiesService
from .graph_client import get_graph_client
from .integrity_service import IntegrityService, integrity_service
from .repository import ValidationRepository
from .users_service import UsersService
from .utils import allowed_keys_from_schema, get_schema_for_document, sanitize_metadata
from .validators import validate_entity_object

logger = logging.getLogger(__name__)


class ValidationService:
    def __init__(
        self,
        repository: Optional[ValidationRepository] = None,
        integrity: Optional[IntegrityService] = None,
        archive: Optional[ArchiveService] = None,
    ):
        graph_client = get_graph_client()
        self._users_service = UsersService(graph_client)
        self._entities_service = EntitiesService(self._users_service)
        self._repository = repository
        self._integrity = integrity or integrity_service
        self._archive = archive or archive_service

    @property
    def repository(self) -> ValidationRepository:
        if self._repository is None:
            self._repository = ValidationRepository()
        return self._repository

    def get_db(self):
        return db_instance.get_db()

    async def dry_run_validation(self, doc_id: str, payload: ValidationRequest):
        db = self.get_db()

        doc = db.collection("documents").get(doc_id)
        if not doc:
            raise ValueError("Documento no encontrado")

        schema = get_schema_for_document(db, doc_id)

        logger.info(f"schema: {schema}")

        if not schema:
            return {"score": 100, "is_ready": True, "fields_report": [], "summary_warnings": ["Sin esquema definido"]}

        fields_report = []
        total_weight = 0
        earned_weight = 0

        input_data = payload.metadata

        logger.info(input_data)

        for field in schema.get("fields", []):
            key = field["fieldKey"]
            label = field.get("label", key)
            is_required = field.get("isRequired", False)
            data_type = field.get("dataType", "string")
            entity_type = (field.get("entityType") or {}).get("key")

            value = input_data.get(key)

            report = {
                "key": key,
                "label": label,
                "is_valid": True,
                "warnings": [],
                "actions": [],
            }

            weight = 2 if is_required else 1
            total_weight += weight

            if is_required and not value:
                report["is_valid"] = False
                report["warnings"].append("Campo obligatorio vacío.")
                fields_report.append(report)
                continue

            if value:
                if data_type == "email":
                    if not re.match(r"[^@]+@[^@]+\.[^@]+", str(value)):
                        report["is_valid"] = False
                        report["warnings"].append("Formato de email inválido.")

                elif data_type == "date":
                    try:
                        datetime.strptime(str(value), "%Y-%m-%d")
                    except ValueError:
                        report["is_valid"] = False
                        report["warnings"].append("Formato de fecha inválido (YYYY-MM-DD).")

                elif data_type == "json":
                    if entity_type and not isinstance(value, dict):
                        report["is_valid"] = False
                        report["warnings"].append(
                            "El campo requiere un objeto con estructura de entidad/usuario (no string libre)."
                        )
                    elif isinstance(value, dict):
                        await validate_entity_object(db, value, entity_type, report)

            if report["is_valid"]:
                earned_weight += weight

            fields_report.append(report)

        score = (earned_weight / total_weight) * 100 if total_weight > 0 else 100

        is_ready = all(f["is_valid"] for f in fields_report)

        return {
            "score": round(score, 1),
            "is_ready": is_ready,
            "fields_report": fields_report,
            "summary_warnings": [f.get("warnings")[0] for f in fields_report if f["warnings"]],
        }

    async def confirm_validation(self, task_id: str, payload: ValidationConfirmRequest, current_user_id: str):
        db = self.get_db()

        doc_snapshot = self.repository.get_document_snapshot(task_id)
        if not doc_snapshot:
            raise ValueError("Documento no encontrado")

        owner_id = doc_snapshot.get("owner_id")
        if owner_id != current_user_id:
            raise PermissionError("Solo el owner del documento puede confirmar")

        storage_data = doc_snapshot.get("storage") or {}
        original_pdf_path = storage_data.get("pdf_original_path")
        selected_pdf_path = storage_data.get("pdf_path")

        storage_for_confirm = dict(storage_data)
        if payload.keep_original:
            if not original_pdf_path:
                raise ValueError("No existe archivo PDF original para usar como principal")
            selected_pdf_path = original_pdf_path

        storage_for_confirm["pdf_path"] = selected_pdf_path
        storage_for_confirm["primary_source"] = "original" if payload.keep_original else "ocr_pdfa"
        storage_for_confirm["pdfa_conversion_required"] = payload.keep_original
        storage_for_confirm["pdfa_conversion_status"] = "pending" if payload.keep_original else None

        needs_archive_promotion = any(
            isinstance(v, str) and ("stage-validate/" in v or "/stage/" in v)
            for v in storage_for_confirm.values()
        )
        if needs_archive_promotion:
            storage_for_confirm = self._archive.promote_from_stage(doc_snapshot, storage_for_confirm)
            selected_pdf_path = storage_for_confirm.get("pdf_path")

        raw_metadata = payload.metadata or {}

        schema = get_schema_for_document(db, task_id)

        metadata_with_ids = await self._entities_service.ensure_entities_exist(db, raw_metadata, schema=schema)
        
        allowed_keys = allowed_keys_from_schema(schema) if schema else None

        clean_metadata = sanitize_metadata(metadata_with_ids, allowed_keys=allowed_keys)

        integrity_payload = self._integrity.build_integrity_payload(
            doc_id=task_id,
            validated_metadata=clean_metadata,
            confirmed_by=current_user_id,
            keep_original=payload.keep_original,
            selected_pdf_path=selected_pdf_path,
        )

        updated_doc = self.repository.confirm_document(
            doc_id=task_id,
            clean_metadata=clean_metadata,
            is_public=payload.is_public,
            display_name=payload.display_name,
            confirmed_by=current_user_id,
            keep_original=payload.keep_original,
            integrity_payload=integrity_payload,
            storage_data=storage_for_confirm,
        )

        if not updated_doc:
            raise ValueError("No se pudo actualizar el documento en confirmación")

        if updated_doc.get("is_public") != payload.is_public:
            raise RuntimeError("La actualización de is_public no se persistió correctamente")

        if payload.display_name is not None and updated_doc.get("display_name") != payload.display_name:
            raise RuntimeError("La actualización de display_name no se persistió correctamente")

        for item in clean_metadata.values():
            if isinstance(item, dict) and item.get("id"):
                self._entities_service.add_semantic_relation(db, task_id, item["id"], "references")

        return {"status": "success", "message": "Documento confirmado y metadata limpiada."}


validation_service = ValidationService()
