import logging
from typing import Any, Dict, Optional

from .users_service import UsersService
from .utils import entity_types_from_schema, is_user_type, looks_like_user_payload, map_type_to_collection

logger = logging.getLogger(__name__)


class EntitiesService:
    def __init__(self, users_service: UsersService):
        self._users_service = users_service

    async def ensure_entities_exist(self, db, metadata: Dict[str, Any], *, schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        entity_type_map = entity_types_from_schema(schema) if schema else {}
        for key, item in (metadata or {}).items():
            if not isinstance(item, dict):
                continue

            if "value" in item and isinstance(item.get("value"), dict):
                val_obj = item["value"]
                target = item["value"]
            else:
                val_obj = item
                target = metadata[key]

            entity_id = val_obj.get("id")
            name = val_obj.get("name") or val_obj.get("display_name")
            type_str = val_obj.get("type") or entity_type_map.get(key)

            if entity_id or not name:
                continue

            if is_user_type(type_str) or looks_like_user_payload(val_obj):
                user_doc = await self._users_service.find_or_create_user(
                    db,
                    display_name=name,
                    email=val_obj.get("email"),
                    guid_ms=val_obj.get("guid_ms"),
                )
                if user_doc:
                    target.update(self._users_service.build_metadata_from_user(user_doc))
                    self._remove_name_fragments(target)
                    logger.info("✨ Usuario encontrado/actualizado: %s", name)
                    continue

                new_id = await self._users_service.create_new_user_node(
                    db,
                    display_name=name,
                    email=val_obj.get("email"),
                )
                target.update(
                    {
                        "id": new_id,
                        "type": "user",
                        "display_name": name,
                    }
                )
                self._remove_name_fragments(target)
                logger.info("✨ Usuario creado al vuelo: %s (%s)", name, new_id)
                continue

            new_id = await self._create_new_entity_node(db, name, type_str)
            target["id"] = new_id
            logger.info("✨ Entidad creada al vuelo: %s (%s)", name, new_id)

        return metadata

    def add_semantic_relation(self, db, task_id: str, entity_id: str, edge_name: str):
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

    async def _create_new_entity_node(self, db, name: str, type_str: Optional[str]) -> str:
        collection = map_type_to_collection(type_str) or "entities"
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

    def _remove_name_fragments(self, value: Dict[str, Any]) -> None:
        for rm in ("first_name", "last_name"):
            value.pop(rm, None)
