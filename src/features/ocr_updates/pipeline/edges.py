import logging

logger = logging.getLogger(__name__)

async def create_structural_edges(db, *, task_id: str, schema_id: str | None, context_entity_id: str | None, context_entity_type: str | None):
    """
    (7) RESTAURACIÓN DE EDGES (RELACIONES ESTRUCTURALES)
    Estas relaciones vienen del JSON externo (contexto inmutable), no del OCR.
    """
    # A. Documento -> Esquema
    if schema_id:
        await create_safe_edge(
            db,
            from_id=f"documents/{task_id}",
            to_id=f"meta_schemas/{schema_id}",
            collection="usa_esquema",
            edge_key=f"{task_id}_{schema_id}",
        )
        logger.info("🔗 Edge creado: documents/%s -> meta_schemas/%s (usa_esquema)", task_id, schema_id)

    # B. Documento -> Entidad organizativa (Carrera / Facultad / etc.)
    if context_entity_id:
        await create_safe_edge(
            db,
            from_id=f"documents/{task_id}",
            to_id=f"entidades/{context_entity_id}",
            collection="pertenece_a",
            edge_key=f"{task_id}_{context_entity_id}",
        )
        logger.info("🔗 Edge creado: documents/%s -> entidades/%s (pertenece_a) [%s]", task_id, context_entity_id, context_entity_type or "entity")


async def create_safe_edge(db, *, from_id: str, to_id: str, collection: str, edge_key: str):
    """
    Crea/actualiza una arista de forma segura con UPSERT.
    - Si no existe la colección edge, la crea.
    - Si existe el edge, solo actualiza updated_at.
    """
    if not db.has_collection(collection):
        db.create_collection(collection, edge=True)

    aql = f"""
    UPSERT {{ _key: @key }}
    INSERT {{
        _key: @key,
        _from: @from_id,
        _to: @to_id,
        created_at: DATE_NOW(),
        updated_at: DATE_NOW()
    }}
    UPDATE {{
        updated_at: DATE_NOW()
    }}
    IN {collection}
    """
    db.aql.execute(
        aql,
        bind_vars={"key": edge_key, "from_id": from_id, "to_id": to_id},
    )
