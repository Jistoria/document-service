from typing import Any, Dict, Optional

from .utils import is_user_type, map_type_to_collection


async def validate_entity_object(db, value_dict: Dict[str, Any], entity_type: Optional[str], report: Dict[str, Any]):
    """
    Valida integridad del objeto JSON de entidad/usuario.
    Para entidades institucionales (collection `entities`) NO se permite creación dinámica.
    """
    entity_id = value_dict.get("id")
    name = value_dict.get("name") or value_dict.get("display_name")

    collection = map_type_to_collection(entity_type)
    is_user = is_user_type(entity_type)

    if not name and not entity_id:
        report["is_valid"] = False
        report["warnings"].append("El objeto no tiene nombre ni id.")
        return

    if entity_id:
        if collection:
            exists = db.collection(collection).has(entity_id)
            if not exists:
                report["is_valid"] = False
                if is_user:
                    report["warnings"].append("Usuario nuevo. Se creará registro al guardar.")
                    report["actions"].append("CREATE_USER")
                else:
                    report["warnings"].append("ID de entidad no encontrado en base de datos local.")
        return

    if not is_user:
        report["is_valid"] = False
        report["warnings"].append(f"Entidad institucional '{name}' sin id. Debe existir previamente.")
        return

    report["warnings"].append(f"Nuevo usuario detectado: '{name}'.")
    report["actions"].append("CREATE_USER")
