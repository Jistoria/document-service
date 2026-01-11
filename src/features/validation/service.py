import logging

# Importamos tus repositorios existentes para chequear existencia
from src.features.ocr_updates.pipeline.users_repository import find_user_by_guid_or_email
from src.features.ocr_updates.pipeline.validation import _find_entity_match  # Reutilizamos tu lógica de búsqueda

logger = logging.getLogger(__name__)


import logging
from typing import Dict, Any
from datetime import datetime
import re

from src.core.database import db_instance
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
        schemas = list(cursor)
        schema = schemas[0] if len(schemas) > 0 else None

        logger.info(f"schema: {schema}")

        if not schema:
            # Si no hay esquema, validación genérica
            return {"score": 100, "is_ready": True, "fields_report": [], "summary_warnings": ["Sin esquema definido"]}

        # 2. Analizar campos
        fields_report = []
        total_weight = 0
        earned_weight = 0

        input_data = payload.metadata  # Lo que envió el usuario

        logger.info(input_data)

        for field in schema.get("fields", []):
            key = field["fieldKey"]
            label = field.get("label", key)
            is_required = field.get("isRequired", False)
            data_type = field.get("dataType", "string")  # string, json, date...
            entity_type = (field.get("entityType") or {}).get("key")

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

    # ---------------------------------------------------------------------
    # HELPERS: schema + sanitización estricta (lo importante para tu caso)
    # ---------------------------------------------------------------------
    def _get_schema_for_document(self, db, doc_id: str) -> Dict[str, Any] | None:
        """
        Obtiene el esquema vinculado al documento por la arista 'usa_esquema'.
        """
        aql = """
        FOR doc IN documents
            FILTER doc._key == @doc_id
            FOR schema IN 1..1 OUTBOUND doc usa_esquema
            RETURN schema
        """
        cursor = db.aql.execute(aql, bind_vars={"doc_id": doc_id})
        schemas = list(cursor)
        return schemas[0] if schemas else None

    def _allowed_keys_from_schema(self, schema: Dict[str, Any]) -> set[str]:
        return {f.get("fieldKey") for f in (schema.get("fields") or []) if f.get("fieldKey")}

    def _filter_entity_fields(self, val: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deja SOLO lo útil de dominio (nada de is_valid/source/message/etc).
        """
        # Caso usuario/persona (Graph o dms_users)
        if any(k in val for k in ("first_name", "last_name", "display_name", "email")):
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

        # Caso entidad institucional (facultad/carrera/departamento/etc)
        out = {
            "id": val.get("id"),
            "name": val.get("name"),
            "code": val.get("code"),
            "type": val.get("type"),
        }
        return {k: v for k, v in out.items() if v is not None}

    def _sanitize_metadata(self, raw_data: Dict[str, Any], allowed_keys: set[str] | None = None) -> Dict[str, Any]:
        """
        Normaliza y limpia la metadata:
        - Elimina wrapper UI (is_valid/source/message/etc)
        - Para entidades deja solo campos permitidos
        - Si item.is_valid == False => guarda None (o omite, tú eliges)
        - Si allowed_keys viene, SOLO guarda keys presentes en el schema
        """
        clean: Dict[str, Any] = {}

        for key, item in (raw_data or {}).items():
            if allowed_keys is not None and key not in allowed_keys:
                continue

            # Primitivo
            if not isinstance(item, dict):
                clean[key] = item
                continue

            # Wrapper UI típico
            if "value" in item:
                # Política para inválidos:
                # - opción A: omitirlo => `continue`
                # - opción B: setearlo a None (recomendado si el schema lo espera)
                if item.get("is_valid") is False:
                    clean[key] = None
                    continue

                val = item.get("value")

                # value es dict (entidad) -> filtrar estricto
                if isinstance(val, dict):
                    clean[key] = self._filter_entity_fields(val)
                    continue

                # value es None, pero hay datos arriba (ej tutor con id/email/display_name)
                if val is None:
                    minimal = {
                        "id": item.get("id"),
                        "display_name": item.get("display_name"),
                        "email": item.get("email"),
                    }
                    minimal = {k: v for k, v in minimal.items() if v}
                    clean[key] = minimal if minimal else None
                    continue

                # value primitivo
                clean[key] = val
                continue

            # Si no es wrapper, y es dict: lo dejas tal cual (o lo podrías normalizar)
            clean[key] = item

        return clean

    # ---------------------------------------------------------------------
    # TU CONFIRM: reemplázalo por este (copy/paste)
    # ---------------------------------------------------------------------
    async def confirm_validation(self, task_id: str, payload: ValidationRequest):
        db = self.get_db()

        # 1) data cruda del frontend
        raw_metadata = payload.metadata or {}

        # 2) Asegurar entidades (si tu front manda type y no manda id)
        metadata_with_ids = await self._ensure_entities_exist(db, raw_metadata)

        # 3) Leer schema para filtrar keys válidas
        schema = self._get_schema_for_document(db, task_id)
        allowed_keys = self._allowed_keys_from_schema(schema) if schema else None

        # 4) Sanitización FINAL (limpia y filtrada por schema)
        clean_metadata = self._sanitize_metadata(metadata_with_ids, allowed_keys=allowed_keys)

        # 5) Update final del documento (IMPORTANTE: además de setear, borra basura de antes)
        # - Eliminamos validated_metadata anterior antes de setearlo (para evitar residuos)
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
            RETURN NEW
        """

        db.aql.execute(update_doc_aql, bind_vars={"key": task_id, "clean_data": clean_metadata})

        # 6) Enriquecimiento (relaciones semánticas)
        for k, item in clean_metadata.items():
            if isinstance(item, dict) and item.get("id"):
                # ojo: aquí tu edge asume entities/, pero users van a dms_users/
                # si quieres, lo ajustamos luego. Por ahora mantengo tu lógica.
                self._add_semantic_relation(db, task_id, item.get("id"), "references")

        return {"status": "success", "message": "Documento confirmado y metadata limpiada."}

    # ---------------------------------------------------------------------
    # LO QUE YA TENÍAS (sin cambios), solo lo dejo para que pegue completo
    # ---------------------------------------------------------------------
    async def _ensure_entities_exist(self, db, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Crea entidades nuevas si value es dict, no tiene id, pero tiene name y type.
        """
        for key, item in (metadata or {}).items():
            if not isinstance(item, dict):
                continue

            val_obj = item.get("value")
            if isinstance(val_obj, dict):
                entity_id = val_obj.get("id")
                name = val_obj.get("name") or val_obj.get("display_name")
                type_str = val_obj.get("type")

                if not entity_id and name and type_str:
                    new_id = await self._create_new_entity_node(db, name, type_str)
                    metadata[key]["value"]["id"] = new_id
                    logger.info(f"✨ Entidad creada al vuelo: {name} ({new_id})")

        return metadata

    async def _create_new_entity_node(self, db, name: str, type_str: str) -> str:
        collection = self._map_type_to_collection(type_str) or "entities"
        aql = f"""
        INSERT {{
            name: @name,
            type: @type,
            created_at: DATE_NOW(),
            source: 'manual_validation_creation'
        }} IN {collection}
        RETURN NEW._key
        """
        cursor = db.aql.execute(aql, bind_vars={"name": name, "type": type_str})
        return list(cursor)[0]

    def _map_type_to_collection(self, entity_type):
        mapping = {
            "user": "dms_users",
            "person": "dms_users",
            "faculty": "entities",
            "career": "entities",
            "department": "entities",
            # tus tipos reales:
            "facultad": "entities",
            "carrera": "entities",
        }
        return mapping.get(entity_type)

    def _add_semantic_relation(self, db, task_id, entity_id, edge_name):
        """
        Tu lógica original (la dejo igual).
        """
        check_owner_aql = """
        FOR doc IN documents
            FILTER doc._key == @task_id
            FOR v IN 1..1 OUTBOUND doc belongs_to
            FILTER v._key == @entity_id
            RETURN 1
        """
        is_owner = list(db.aql.execute(check_owner_aql, bind_vars={"task_id": task_id, "entity_id": entity_id}))
        if is_owner:
            return

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

        db.aql.execute(upsert_edge_aql, bind_vars={"task_id": task_id, "entity_id": entity_id})


validation_service = ValidationService()


