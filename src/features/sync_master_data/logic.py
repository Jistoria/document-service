from arango.database import StandardDatabase
from .models import MasterDataExport


async def sync_to_arango(db: StandardDatabase, data: MasterDataExport):
    print(f"🔄 Sincronizando Grafos (Estructura y Esquemas)...")

    # -------------------------------------------------------
    # 1. SINCRONIZAR ESTRUCTURA (Sede -> Dept -> Carrera)
    # -------------------------------------------------------
    # data.structure ahora es una lista, iteramos directamente
    for sede in data.structure:
        # A. Nodo Sede
        await _upsert_entity(db, sede.id, sede.name, 'sede', sede.code, sede.code_numeric)

        for dept in sede.departments:
            # B. Nodo Facultad/Departamento
            # Guardamos 'code' (ej: FCVT) también
            await _upsert_entity(db, dept.id, dept.name, 'facultad', dept.code, dept.code_numeric)

            # C. Relación: Facultad -> PERTENECE_A -> Sede
            await _upsert_edge(db, dept.id, sede.id, 'belongs_to')

            for car in dept.careers:
                # D. Nodo Carrera
                await _upsert_entity(db, car.id, car.name, 'carrera', car.code, car.code_numeric)

                # E. Relación: Carrera -> PERTENECE_A -> Facultad
                await _upsert_edge(db, car.id, dept.id, 'belongs_to')

    # -------------------------------------------------------
    # 2. SINCRONIZAR ESQUEMAS
    # -------------------------------------------------------
    aql_schemas = """
    FOR s IN @schemas
        UPSERT { _key: s.id }
        INSERT { 
            _key: s.id, 
            name: s.name, 
            version: s.version, 
            fields: s.metadataFields 
        }
        UPDATE { 
            name: s.name, 
            version: s.version, 
            fields: s.metadataFields 
        }
        IN meta_schemas
    """

    # Convertimos a dict para pasarlo a Arango
    schemas_list = [s.model_dump() for s in data.schemas]

    if schemas_list:
        db.aql.execute(aql_schemas, bind_vars={'schemas': schemas_list})

    print("✅ Sincronización completada exitosamente.")


# --- Funciones Auxiliares (Helpers) ---

async def _upsert_entity(db, uuid, name, type_label, code=None, code_numeric=None):
    """Crea o actualiza un nodo de entidad (Sede, Facultad, Carrera)"""
    aql = """
    UPSERT { _key: @key }
    INSERT { 
        _key: @key, 
        name: @name, 
        type: @type, 
        label: @name,
        code: @code,
        code_numeric: @code_numeric,
    }
    UPDATE { 
        name: @name, 
        label: @name,
        code: @code,
        code_numeric: @code_numeric
    }
    IN entities
    """
    db.aql.execute(aql, bind_vars={
        'key': uuid,
        'name': name,
        'type': type_label,
        'code': code,
        'code_numeric': code_numeric,
    })


async def _upsert_edge(db, child_id, parent_id, collection):
    """Crea o actualiza una relación entre entities"""
    # Usamos una clave compuesta para evitar aristas duplicadas
    edge_key = f"{child_id}_{parent_id}"

    aql = f"""
    UPSERT {{ _key: @key }}
    INSERT {{ 
        _key: @key, 
        _from: CONCAT('entities/', @child), 
        _to: CONCAT('entities/', @parent) 
    }}
    UPDATE {{ }}
    IN {collection}
    """
    db.aql.execute(aql, bind_vars={
        'key': edge_key,
        'child': child_id,
        'parent': parent_id
    })