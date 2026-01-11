from typing import Any, Dict, Optional

from .utils import is_user_type, map_type_to_collection


async def validate_entity_object(db, value_dict: Dict[str, Any], entity_type: Optional[str], report: Dict[str, Any]):
    """
    Valida si el objeto JSON (usuario, carrera) existe en BD o necesita crearse.
    """
    entity_id = value_dict.get("id")
    name = value_dict.get("name") or value_dict.get("display_name")

    if not name:
        report["is_valid"] = False
        report["warnings"].append("El objeto no tiene nombre.")
        return

    if entity_id:
        collection = map_type_to_collection(entity_type)
        if collection:
            exists = db.collection(collection).has(entity_id)
            if not exists:
                if is_user_type(entity_type):
                    report["warnings"].append("Usuario nuevo. Se creará registro al guardar.")
                    report["actions"].append("CREATE_USER")
                else:
                    report["warnings"].append("ID de entidad no encontrado en base de datos local.")
                    report["actions"].append("CREATE_ENTITY")
        return

    report["warnings"].append(f"Nuevo registro detectado: '{name}'.")
    report["actions"].append("CREATE_ENTITY")
