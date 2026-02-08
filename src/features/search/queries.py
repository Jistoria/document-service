"""
Queries AQL para el módulo de búsqueda de documentos.
Contiene todas las queries de ArangoDB separadas de la lógica de negocio.
"""

class SearchQueries:
    """Clase que contiene todas las queries AQL para búsqueda de documentos."""
    
    @staticmethod
    def get_document_by_id_query() -> str:
        """
        Query para obtener un documento por su ID incluyendo sus relaciones.
        
        Returns:
            str: Query AQL con placeholders para bind_vars
        """
        return """
        FOR doc IN documents
            FILTER doc._key == @doc_id

            // 1. Buscar Entidad (Ubicación)
            LET entity = (
                FOR v IN 1..1 OUTBOUND doc file_located_in
                RETURN { id: v._key, name: v.name, type: v.type, code: v.code }
            )[0]

            // 2. Buscar Esquema
            LET schema = (
                FOR v IN 1..1 OUTBOUND doc usa_esquema
                RETURN { id: v._key, name: v.name, version: v.version }
            )[0]

            // 3. Buscar Documento Requerido (Definición)
            LET req_doc = (
                FOR v IN 1..1 OUTBOUND doc complies_with
                RETURN { id: v._key, name: v.name, code_default: v.code }
            )[0]

            RETURN MERGE(doc, { 
                context_entity: entity, 
                used_schema: schema,
                required_document: req_doc
            })
        """
    
    @staticmethod
    def search_documents_query(filter_clause: str) -> str:
        """
        Query para búsqueda paginada de documentos con filtros dinámicos.
        
        Args:
            filter_clause: Cláusula FILTER construida dinámicamente
            
        Returns:
            str: Query AQL completa con subqueries de relaciones
        """
        return f"""
        LET docs = (
            FOR doc IN documents
                {filter_clause}
                SORT doc.created_at DESC
                LIMIT @offset, @limit

                LET entity = (
                    FOR v IN 1..1 OUTBOUND doc file_located_in
                    RETURN {{ id: v._key, name: v.name, type: v.type }}
                )[0]

                LET schema = (
                    FOR v IN 1..1 OUTBOUND doc usa_esquema
                    RETURN {{ id: v._key, name: v.name }}
                )[0]

                LET req_doc = (
                    FOR v IN 1..1 OUTBOUND doc complies_with
                    RETURN {{ id: v._key, name: v.name, code_default: v.code }}
                )[0]

                RETURN MERGE(doc, {{
                    context_entity: entity,
                    used_schema: schema,
                    required_document: req_doc
                }})
        )

        LET total_count = (
            FOR doc IN documents
                {filter_clause}
                RETURN 1
        )

        RETURN {{ items: docs, total: LENGTH(total_count) }}
        """
    
    @staticmethod
    def get_entities_with_documents_query() -> str:
        """
        Query para obtener todas las entidades que tienen documentos asociados.
        
        Returns:
            str: Query AQL que retorna entidades únicas
        """
        return """
        FOR doc IN documents
            FOR entity IN 1..1 OUTBOUND doc file_located_in
            RETURN DISTINCT {
                id: entity._key,
                name: entity.name,
                type: entity.type
            }
        """
