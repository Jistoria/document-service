from src.core.database import db_instance
from .models import ValidationRequest
from datetime import datetime


class ValidationService:

    def get_db(self):
        return db_instance.get_db()

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