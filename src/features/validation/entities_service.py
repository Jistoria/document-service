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

        logger.info("entity_type_map: %s", entity_type_map)
        logger.info("📋 Processing metadata keys: %s", list(metadata.keys()) if metadata else [])
        
        for key, item in (metadata or {}).items():
            logger.info("🔍 Processing key='%s', item type=%s", key, type(item).__name__)

            expected_type = entity_type_map.get(key)

            if not isinstance(item, dict):
                if is_user_type(expected_type) and isinstance(item, str) and item.strip():
                    logger.info("  👤 Resolviendo usuario escalar para '%s': %s", key, item)
                    user_doc = await self._users_service.find_or_create_user(
                        db,
                        display_name=item.strip(),
                        email=None,
                        guid_ms=None,
                    )

                    if user_doc:
                        metadata[key] = self._users_service.build_metadata_from_user(user_doc)
                        logger.info("  ✅ Usuario resuelto para '%s'", key)
                        continue

                    new_id = await self._users_service.create_new_user_node(
                        db,
                        display_name=item.strip(),
                    )
                    metadata[key] = {
                        "id": new_id,
                        "type": "user",
                        "display_name": item.strip(),
                    }
                    logger.info("  ✅ Usuario creado para '%s': %s", key, new_id)
                    continue

                logger.info("  ⏭️  Skipping '%s': not a dict", key)
                continue

            if "value" in item and isinstance(item.get("value"), dict):
                val_obj = item["value"]
                target = item["value"]
                logger.info("  📦 Found wrapped value for '%s'", key)
            else:
                val_obj = item
                target = metadata[key]
                logger.info("  📦 Using direct value for '%s'", key)

            entity_id = val_obj.get("id")
            name = val_obj.get("name") or val_obj.get("display_name")
            type_str = val_obj.get("type") or expected_type
            
            logger.info("  🏷️  key='%s': id=%s, name=%s, type=%s", key, entity_id, name, type_str)

            is_user_type_result = is_user_type(type_str)
            looks_like_user_result = looks_like_user_payload(val_obj)
            logger.info("  🔎 Checking if '%s' is user: is_user_type=%s, looks_like_user=%s", 
                       key, is_user_type_result, looks_like_user_result)
            
            # For users, ALWAYS validate integrity even if they have an id
            if is_user_type_result or looks_like_user_result:
                logger.info("  👤 Processing as USER: %s (existing_id=%s)", name, entity_id)
                
                # If user already has an id, verify it exists in the database
                if entity_id:
                    user_exists = await self._users_service.verify_user_exists(db, entity_id)
                    if user_exists:
                        logger.info(" Usuario verificado en BD: %s (%s)", name, entity_id)
                        continue
                    else:
                        logger.warning("⚠️  Usuario con id=%s NO existe en BD, buscando/creando...", entity_id)
                
                # Search or create user
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

            collection = map_type_to_collection(type_str) or "entities"

            if not entity_id:
                raise ValueError(
                    f"El campo '{key}' requiere una entidad existente (id obligatorio)."
                )

            exists = db.collection(collection).has(entity_id)
            if not exists:
                raise ValueError(
                    f"La entidad '{entity_id}' del campo '{key}' no existe en '{collection}'."
                )

            logger.info("  ✅ Entidad verificada en '%s': %s", collection, entity_id)

            entity_doc = db.collection(collection).get(entity_id)
            if entity_doc and entity_doc.get("code_numeric") is not None:
                target["code_numeric"] = entity_doc.get("code_numeric")

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

    def _remove_name_fragments(self, value: Dict[str, Any]) -> None:
        for rm in ("first_name", "last_name"):
            value.pop(rm, None)
