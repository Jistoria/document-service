from arango.database import StandardDatabase
from .models import MasterDataExport, ProcessSync


async def sync_to_arango(db: StandardDatabase, data: MasterDataExport):
    print(f"🔄 Sincronizando Grafos Completo...")

    # 1. SINCRONIZAR ESTRUCTURA (Sedes -> Carreras)
    await _sync_structure(db, data.structure)

    # 2. SINCRONIZAR ESQUEMAS
    await _sync_schemas(db, data.schemas)

    # 3. SINCRONIZAR CATÁLOGO DE PROCESOS (NUEVO)
    await _sync_catalog(db, data.catalog)

    print("✅ Sincronización completada exitosamente.")


# --- Lógica de Estructura (Ya la tenías, encapsulada para orden) ---
async def _sync_structure(db, structure):
    for sede in structure:
        await _upsert_entity(db, sede.id, sede.name, 'sede', sede.code, sede.code_numeric)
        for dept in sede.departments:
            await _upsert_entity(db, dept.id, dept.name, 'facultad', dept.code, dept.code_numeric)
            await _upsert_edge(db, dept.id, sede.id, 'belongs_to', 'entities')  # Child -> Parent
            for car in dept.careers:
                await _upsert_entity(db, car.id, car.name, 'carrera', car.code, car.code_numeric)
                await _upsert_edge(db, car.id, dept.id, 'belongs_to', 'entities')


# --- Lógica de Esquemas (Ya la tenías) ---
async def _sync_schemas(db, schemas):
    aql_schemas = """
    FOR s IN @schemas
        UPSERT { _key: s.id }
        INSERT { _key: s.id, name: s.name, version: s.version, fields: s.metadataFields }
        UPDATE { name: s.name, version: s.version, fields: s.metadataFields }
        IN meta_schemas
    """
    schemas_list = [s.model_dump() for s in schemas]
    if schemas_list:
        db.aql.execute(aql_schemas, bind_vars={'schemas': schemas_list})


# --- Lógica de Catálogo (NUEVO) ---
async def _sync_catalog(db, catalog):
    # Usaremos una colección genérica 'catalog_nodes' para todo el árbol de procesos
    # O podemos usar 'entities' si queremos un grafo unificado, pero mejor separar.
    # Vamos a usar colecciones dedicadas para claridad:
    # - subsystems
    # - process_categories
    # - processes
    # - required_documents

    # Asegurar colecciones
    for col in ['subsystems', 'process_categories', 'processes', 'required_documents']:
        if not db.has_collection(col): db.create_collection(col)

    if not db.has_collection('catalog_belongs_to'): db.create_collection('catalog_belongs_to', edge=True)

    for sub in catalog:
        # A. Subsistema
        await _upsert_node(db, 'subsystems', sub.id, sub.name, sub.code)

        for cat in sub.processCategories:
            # B. Categoría
            await _upsert_node(db, 'process_categories', cat.id, cat.name, cat.code)
            await _upsert_catalog_edge(db, f"process_categories/{cat.id}", f"subsystems/{sub.id}")

            for proc in cat.processes:
                # C. Proceso (Raíz)
                await _sync_process_recursive(db, proc, f"process_categories/{cat.id}")


async def _sync_process_recursive(db, process: ProcessSync, parent_id_str: str):
    # Guardar Proceso
    await _upsert_node(db, 'processes', process.id, process.name, process.code)

    # Conectar con Padre (Categoría u otro Proceso)
    await _upsert_catalog_edge(db, f"processes/{process.id}", parent_id_str)

    # Procesar Documentos Requeridos
    for req_doc in process.requiredDocuments:
        # Guardar Nodo Documento Requerido
        await _upsert_node(db, 'required_documents', req_doc.id, req_doc.name, req_doc.codeDefault,
                           extra={'schema_id': req_doc.metadataSchemaId})

        # Conectar Doc -> Proceso
        await _upsert_catalog_edge(db, f"required_documents/{req_doc.id}", f"processes/{process.id}")

        # Conectar Doc -> Schema (Si tiene) - Esto es CRÍTICO para validar
        if req_doc.metadataSchemaId:
            await _upsert_edge_generic(db, 'usa_esquema',
                                       f"required_documents/{req_doc.id}",
                                       f"meta_schemas/{req_doc.metadataSchemaId}")

    # Recursividad para Subprocesos
    for sub_proc in process.subProcesses:
        await _sync_process_recursive(db, sub_proc, f"processes/{process.id}")


# --- Helpers Genéricos ---

async def _upsert_entity(db, uuid, name, type_label, code=None, code_numeric=None):
    """Para Organizational Structure (entities)"""
    aql = """
    UPSERT { _key: @key }
    INSERT { _key: @key, name: @name, type: @type, label: @name, code: @code, code_numeric: @code_numeric }
    UPDATE { name: @name, label: @name, code: @code, code_numeric: @code_numeric }
    IN entities
    """
    db.aql.execute(aql, bind_vars={'key': uuid, 'name': name, 'type': type_label, 'code': code, 'code_numeric':code_numeric})


async def _upsert_node(db, collection, uuid, name, code, extra=None):
    """Para Catalog Nodes (processes, etc)"""
    doc = {'_key': uuid, 'name': name, 'code': code}
    if extra: doc.update(extra)

    aql = f"""
    UPSERT {{ _key: @key }}
    INSERT @doc
    UPDATE @doc
    IN {collection}
    """
    db.aql.execute(aql, bind_vars={'key': uuid, 'doc': doc})


async def _upsert_edge(db, child_uuid, parent_uuid, collection, node_coll='entities'):
    """Helper legacy para structure"""
    await _upsert_edge_generic(db, collection, f"{node_coll}/{child_uuid}", f"{node_coll}/{parent_uuid}")


async def _upsert_catalog_edge(db, child_id_str, parent_id_str):
    """Helper para catalog (usa la colección catalog_belongs_to)"""
    await _upsert_edge_generic(db, 'catalog_belongs_to', child_id_str, parent_id_str)


async def _upsert_edge_generic(db, collection, from_id, to_id):
    if not db.has_collection(collection): db.create_collection(collection, edge=True)

    # Generar key determinista para evitar duplicados
    edge_key = f"{from_id.split('/')[-1]}_{to_id.split('/')[-1]}"

    aql = f"""
    UPSERT {{ _key: @key }}
    INSERT {{ _key: @key, _from: @from_id, _to: @to_id }}
    UPDATE {{ _to: @to_id }} // Actualizar padre si cambió
    IN {collection}
    """
    db.aql.execute(aql, bind_vars={'key': edge_key, 'from_id': from_id, 'to_id': to_id})