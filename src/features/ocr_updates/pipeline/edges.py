import logging

logger = logging.getLogger(__name__)


async def create_structural_edges(
    db,
    *,
    task_id: str,
    schema_id: str | None,
    context_entity_id: str | None,
    context_entity_type: str | None,
    required_doc_id: str | None
):
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
            to_id=f"entities/{context_entity_id}",
            collection="file_located_in",
            edge_key=f"{task_id}_{context_entity_id}",
        )
        logger.info("🔗 Edge creado: documents/%s -> entities/%s (file_located_in) [%s]", task_id, context_entity_id, context_entity_type or "entity")

    # C. Document -> RequiredDocument (complies_with)
    if required_doc_id:
        # Asumimos que el Management sincronizó estos nodos en la colección 'required_documents'
        await create_safe_edge(
            db,
            from_id=f"documents/{task_id}",
            to_id=f"required_documents/{required_doc_id}",
            collection="complies_with",
            edge_key=f"{task_id}_{required_doc_id}",
        )
        logger.info(f"🔗 Edge creado: documents/{task_id} -> required_documents/{required_doc_id} (complies_with)")


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
