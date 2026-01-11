import logging
from datetime import datetime
import re

from src.core.database import db_instance
from src.features.validation.models import ValidationRequest

from .entities_service import EntitiesService
from .graph_client import get_graph_client
from .users_service import UsersService
from .utils import allowed_keys_from_schema, get_schema_for_document, sanitize_metadata
from .validators import validate_entity_object

logger = logging.getLogger(__name__)


class ValidationService:
    def __init__(self):
        graph_client = get_graph_client()
        self._users_service = UsersService(graph_client)
        self._entities_service = EntitiesService(self._users_service)

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

                elif data_type == "json" and isinstance(value, dict):
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

    async def confirm_validation(self, task_id: str, payload: ValidationRequest):
        db = self.get_db()

        raw_metadata = payload.metadata or {}

        schema = get_schema_for_document(db, task_id)

        metadata_with_ids = await self._entities_service.ensure_entities_exist(db, raw_metadata, schema=schema)
        allowed_keys = allowed_keys_from_schema(schema) if schema else None

        clean_metadata = sanitize_metadata(metadata_with_ids, allowed_keys=allowed_keys)

        update_doc_aql = """
        FOR d IN documents
            FILTER d._key == @key
            UPDATE d WITH {
                validated_metadata: @clean_data,
                status: 'confirmed',
                integrity_warnings: [],
                manually_validated_at: DATE_NOW(),
                is_locked: true
            } IN documents
            OPTIONS { mergeObjects: false }
            RETURN NEW
        """
        db.aql.execute(update_doc_aql, bind_vars={"key": task_id, "clean_data": clean_metadata})

        for item in clean_metadata.values():
            if isinstance(item, dict) and item.get("id"):
                self._entities_service.add_semantic_relation(db, task_id, item["id"], "references")

        return {"status": "success", "message": "Documento confirmado y metadata limpiada."}


validation_service = ValidationService()
