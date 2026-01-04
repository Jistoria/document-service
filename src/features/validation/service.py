import logging
from typing import Dict, Any, List
from datetime import datetime
import re

from src.core.database import db_instance
# Importamos tus repositorios existentes para chequear existencia
from src.features.ocr_updates.pipeline.users_repository import find_user_by_guid_or_email
from src.features.ocr_updates.pipeline.validation import _find_entity_match  # Reutilizamos tu lógica de búsqueda
from src.features.validation.models import ValidationRequest

logger = logging.getLogger(__name__)


class ValidationService:
    def get_db(self):
        return db_instance.get_db()

    async def dry_run_validation(self, doc_id: str, payload: ValidationRequest):
        db = self.get_db()

        # 1. Obtener documento y su esquema
        doc = db.collection("documents").get(doc_id)
        if not doc:
            raise ValueError("Documento no encontrado")

        # Asumimos que el esquema está cacheado en el doc o lo buscamos por el edge 'usa_esquema'
        # Para este ejemplo, buscamos el esquema vinculado
        aql = """
        FOR doc IN documents
            FILTER doc._key == @doc_id
            FOR schema IN 1..1 OUTBOUND doc usa_esquema
            RETURN schema
        """
        cursor = db.aql.execute(aql, bind_vars={"doc_id": doc_id})
        schema = list(cursor)[0] if cursor.count() > 0 else None

        if not schema:
            # Si no hay esquema, validación genérica
            return {"score": 100, "is_ready": True, "fields_report": [], "summary_warnings": ["Sin esquema definido"]}

        # 2. Analizar campos
        fields_report = []
        total_weight = 0
        earned_weight = 0

        input_data = payload.metadata  # Lo que envió el usuario

        for field in schema.get("fields", []):
            key = field["fieldKey"]
            label = field.get("label", key)
            is_required = field.get("isRequired", False)
            data_type = field.get("dataType", "string")  # string, json, date...
            entity_type = field.get("entityType", {}).get("key")  # user, career...

            value = input_data.get(key)

            report = {
                "key": key,
                "label": label,
                "is_valid": True,
                "warnings": [],
                "actions": []
            }

            # PONDERACIÓN: Campos requeridos valen más
            weight = 2 if is_required else 1
            total_weight += weight

            # --- VALIDACIÓN 1: Requerido ---
            if is_required and not value:
                report["is_valid"] = False
                report["warnings"].append("Campo obligatorio vacío.")
                fields_report.append(report)
                continue  # No seguimos validando si está vacío

            # --- VALIDACIÓN 2: Tipos de Dato (PHP MetadataFieldDataType) ---
            if value:
                if data_type == "email":
                    if not re.match(r"[^@]+@[^@]+\.[^@]+", str(value)):
                        report["is_valid"] = False
                        report["warnings"].append("Formato de email inválido.")

                elif data_type == "date":
                    # Validar formato YYYY-MM-DD
                    try:
                        datetime.strptime(str(value), "%Y-%m-%d")
                    except ValueError:
                        report["is_valid"] = False
                        report["warnings"].append("Formato de fecha inválido (YYYY-MM-DD).")

                elif data_type == "json" and isinstance(value, dict):
                    # Validación profunda de objetos (Entidades/Personas)
                    await self._validate_entity_object(db, value, entity_type, report)

            # CALCULAR PUNTAJE PARCIAL
            if report["is_valid"]:
                earned_weight += weight

            fields_report.append(report)

        # 3. Resultado Final
        score = (earned_weight / total_weight) * 100 if total_weight > 0 else 100

        # Umbral: Por ejemplo, requerimos 100% en obligatorios para "is_ready"
        # O simplemente > 80% global. Usemos 100% de validez técnica.
        is_ready = all(f["is_valid"] for f in fields_report)

        return {
            "score": round(score, 1),
            "is_ready": is_ready,
            "fields_report": fields_report,
            "summary_warnings": [f.get("warnings")[0] for f in fields_report if f["warnings"]]
        }

    async def _validate_entity_object(self, db, value_dict, entity_type, report):
        """
        Valida si el objeto JSON (usuario, carrera) existe en BD o necesita crearse.
        """
        # Estructura esperada: { id: "...", name: "..." }
        entity_id = value_dict.get("id")
        name = value_dict.get("name") or value_dict.get("display_name")

        if not name:
            report["is_valid"] = False
            report["warnings"].append("El objeto no tiene nombre.")
            return

        # Si tiene ID, verificamos que exista en la colección correcta
        if entity_id:
            collection = self._map_type_to_collection(entity_type)
            if collection:
                exists = db.collection(collection).has(entity_id)
                if not exists:
                    # Caso raro: Tiene ID pero no está en BD (quizás ID de Microsoft Graph nuevo)
                    if entity_type in ["user", "person"]:
                        report["warnings"].append("Usuario nuevo. Se creará registro al guardar.")
                        report["actions"].append("CREATE_USER")
                    else:
                        report["warnings"].append("ID de entidad no encontrado en base de datos local.")
                        report["actions"].append("CREATE_ENTITY")
        else:
            # No tiene ID, es texto libre o nuevo
            report["warnings"].append(f"Nuevo registro detectado: '{name}'.")
            report["actions"].append("CREATE_ENTITY")

    def _map_type_to_collection(self, entity_type):
        mapping = {
            "user": "dms_users",
            "person": "dms_users",
            "faculty": "entities",  # Asumiendo que facultades y carreras viven en 'entities'
            "career": "entities",
            "department": "entities"
        }
        return mapping.get(entity_type)

    def confirm_validation(self, task_id: str, payload: ValidationRequest):
        db = self.get_db()

        # 1. Actualizar Metadatos del JSON (Igual que antes)
        # Esto es lo que ve el usuario en el formulario
        update_doc_aql = """
        UPDATE @key WITH { 
            validated_metadata: @metadata,
            status: 'confirmed',
            integrity_warnings: [],
            manually_validated_at: DATE_NOW()
        } IN documents
        RETURN NEW
        """
        db.aql.execute(update_doc_aql, bind_vars={
            "key": task_id,
            "metadata": payload.validated_metadata
        })

        # 2. ENRIQUECIMIENTO SEMÁNTICO (Sin borrar padres)
        # Si el usuario validó campos que resultaron ser Entidades (Carrera, Facultad),
        # creamos una relación de "referencia" o "participación".

        # Extraemos las entities validadas del payload
        validated_data = payload.validated_metadata

        for key, item in validated_data.items():
            # Verificamos si el item es válido y tiene estructura de entidad (tiene ID)
            if item.get("is_valid") and isinstance(item.get("value"), dict):
                entity_info = item["value"]
                entity_id = entity_info.get("id")

                # Si tiene ID, es una entidad del sistema (Carrera/Facultad/Sede)
                if entity_id:
                    self._add_semantic_relation(db, task_id, entity_id, "references")

        return {"status": "success", "message": "Documento validado y referencias creadas"}

    def _add_semantic_relation(self, db, task_id, entity_id, edge_name):
        """
        Crea una arista secundaria (NO de pertenencia) para indicar relación/participación.
        Evita duplicados: Si ya pertenece a esa entidad, no crea la referencia para no redundar.
        """

        # Verificar si YA existe una relación de 'belongs_to' con esa misma entidad
        # (Para no decir que "Software referencia a Software" si ya es su dueño)
        check_owner_aql = """
        FOR doc IN documents
            FILTER doc._key == @task_id
            FOR v IN 1..1 OUTBOUND doc belongs_to
            FILTER v._key == @entity_id
            RETURN 1
        """
        is_owner = list(db.aql.execute(check_owner_aql, bind_vars={"task_id": task_id, "entity_id": entity_id}))

        if is_owner:
            return  # Si ya es el dueño, no hacemos nada extra.

        # Si no es el dueño, creamos la relación de referencia/participación
        if not db.has_collection(edge_name):
            db.create_collection(edge_name, edge=True)

        upsert_edge_aql = f"""
        UPSERT {{ _from: CONCAT('documents/', @task_id), _to: CONCAT('entities/', @entity_id) }}
        INSERT {{ 
            _from: CONCAT('documents/', @task_id), 
            _to: CONCAT('entities/', @entity_id),
            created_at: DATE_NOW(),
            source: 'manual_validation'
        }}
        UPDATE {{ updated_at: DATE_NOW() }}
        IN {edge_name}
        """

        db.aql.execute(upsert_edge_aql, bind_vars={
            "task_id": task_id,
            "entity_id": entity_id
        })


validation_service = ValidationService()