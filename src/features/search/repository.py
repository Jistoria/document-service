# src/features/search/repository.py
import math
from typing import List, Dict, Any, Optional
from src.core.database import db_instance
from .models import DocumentDetail, EntityRef

class SearchRepository:
    def __init__(self):
        self.db = db_instance.get_db()

    def get_by_id(self, doc_id: str) -> Optional[Dict[str, Any]]:
        aql = """
        FOR doc IN documents
            FILTER doc._key == @doc_id
            
            LET entity = (
                FOR v IN 1..1 OUTBOUND doc file_located_in
                RETURN { id: v._key, name: v.name, type: v.type, code: v.code }
            )[0]

            LET schema = (
                FOR v IN 1..1 OUTBOUND doc usa_esquema
                RETURN { id: v._key, name: v.name, version: v.version }
            )[0]

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
        cursor = self.db.aql.execute(aql, bind_vars={"doc_id": doc_id})
        result = list(cursor)
        return result[0] if result else None

    def search(
        self, 
        offset: int, 
        limit: int, 
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Construye y ejecuta la query dinámica de búsqueda.
        """
        aql_filters = []
        bind_vars = {"offset": offset, "limit": limit}

        # 1. Filtros de Seguridad (Owner IDs)
        if "valid_owner_ids" in filters:
            aql_filters.append("""
                LENGTH(
                    FOR owner IN 1..2 OUTBOUND doc file_located_in, belongs_to
                    FILTER owner._key IN @valid_owner_ids
                    LIMIT 1
                    RETURN 1
                ) > 0
            """)
            bind_vars["valid_owner_ids"] = filters["valid_owner_ids"]

        # 2. Filtros de Estado y Usuario
        if "status" in filters:
            aql_filters.append("doc.status == @status")
            bind_vars["status"] = filters["status"]

        if "current_user_id" in filters:
            aql_filters.append("doc.owner.id == @current_user_id")
            bind_vars["current_user_id"] = filters["current_user_id"]

        # 3. Filtro Jerárquico de Entidad
        if "entity_id" in filters:
            aql_filters.append("""
                LENGTH(
                    FOR entity IN 1..5 OUTBOUND doc file_located_in, belongs_to
                    FILTER entity._key == @entity_id
                    LIMIT 1
                    RETURN 1
                ) > 0
            """)
            bind_vars["entity_id"] = filters["entity_id"]

        # 4. Filtro Jerárquico de Proceso
        if "process_id" in filters:
            aql_filters.append("""
                LENGTH(
                    FOR node IN 1..6 OUTBOUND doc complies_with, catalog_belongs_to
                    FILTER node._key == @process_id
                    LIMIT 1
                    RETURN 1
                ) > 0
            """)
            bind_vars["process_id"] = filters["process_id"]

        # Unir filtros
        filter_clause = "FILTER " + " AND ".join(aql_filters) if aql_filters else ""

        aql = f"""
        LET docs = (
            FOR doc IN documents
                {filter_clause}
                SORT doc.created_at DESC
                LIMIT @offset, @limit
                
                // Subqueries de relaciones
                LET entity = (FOR v IN 1..1 OUTBOUND doc file_located_in RETURN {{ id: v._key, name: v.name, type: v.type }})[0]
                LET schema = (FOR v IN 1..1 OUTBOUND doc usa_esquema RETURN {{ id: v._key, name: v.name }})[0]
                LET req_doc = (FOR v IN 1..1 OUTBOUND doc complies_with RETURN {{ id: v._key, name: v.name, code_default: v.code }})[0]

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

        cursor = self.db.aql.execute(aql, bind_vars=bind_vars)
        return list(cursor)[0] if cursor else {"items": [], "total": 0}

    def get_entities_with_docs(self) -> List[Dict[str, Any]]:
        aql = """
        FOR doc IN documents
            FOR entity IN 1..1 OUTBOUND doc file_located_in
            RETURN DISTINCT {
                id: entity._key,
                name: entity.name,
                type: entity.type
            }
        """
        cursor = self.db.aql.execute(aql)
        return list(cursor)