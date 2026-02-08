from datetime import date
from typing import Any, Dict, List, Optional

from src.core.database import db_instance


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

    def search(self, offset: int, limit: int, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Construye y ejecuta query AQL dinámica de búsqueda."""
        aql_filters: List[str] = []
        bind_vars: Dict[str, Any] = {"offset": offset, "limit": limit}

        self._add_filter_if_present(
            filters,
            "valid_owner_ids",
            aql_filters,
            bind_vars,
            """
            LENGTH(
                FOR owner IN 1..2 OUTBOUND doc file_located_in, belongs_to
                FILTER owner._key IN @valid_owner_ids
                LIMIT 1
                RETURN 1
            ) > 0
            """,
        )

        self._add_filter_if_present(
            filters,
            "status",
            aql_filters,
            bind_vars,
            "doc.status == @status",
        )

        self._add_filter_if_present(
            filters,
            "current_user_id",
            aql_filters,
            bind_vars,
            "doc.owner.id == @current_user_id",
        )

        self._add_filter_if_present(
            filters,
            "owner_id",
            aql_filters,
            bind_vars,
            "doc.owner.id == @owner_id",
        )

        self._add_filter_if_present(
            filters,
            "entity_id",
            aql_filters,
            bind_vars,
            """
            LENGTH(
                FOR entity IN 1..5 OUTBOUND doc file_located_in, belongs_to
                FILTER entity._key == @entity_id
                LIMIT 1
                RETURN 1
            ) > 0
            """,
        )

        self._add_filter_if_present(
            filters,
            "process_id",
            aql_filters,
            bind_vars,
            """
            LENGTH(
                FOR node IN 1..6 OUTBOUND doc complies_with, catalog_belongs_to
                FILTER node._key == @process_id
                LIMIT 1
                RETURN 1
            ) > 0
            """,
        )

        if filters.get("search"):
            aql_filters.append(
                """
                (
                    CONTAINS(LOWER(doc.naming.display_name), LOWER(@search))
                    OR CONTAINS(LOWER(doc.original_filename), LOWER(@search))
                )
                """
            )
            bind_vars["search"] = filters["search"]

        self._add_filter_if_present(
            filters,
            "required_document_id",
            aql_filters,
            bind_vars,
            """
            LENGTH(
                FOR req IN 1..1 OUTBOUND doc complies_with
                FILTER req._key == @required_document_id
                LIMIT 1
                RETURN 1
            ) > 0
            """,
        )

        self._add_filter_if_present(
            filters,
            "referenced_entity_id",
            aql_filters,
            bind_vars,
            """
            LENGTH(
                FOR entity IN 1..1 OUTBOUND doc references
                FILTER entity._key == @referenced_entity_id
                LIMIT 1
                RETURN 1
            ) > 0
            """,
        )

        self._add_filter_if_present(
            filters,
            "schema_id",
            aql_filters,
            bind_vars,
            """
            LENGTH(
                FOR schema IN 1..1 OUTBOUND doc usa_esquema
                FILTER schema._key == @schema_id
                LIMIT 1
                RETURN 1
            ) > 0
            """,
        )

        self._add_date_filters(filters, aql_filters, bind_vars)

        filter_clause = f"FILTER {' AND '.join(aql_filters)}" if aql_filters else ""

        aql = f"""
        LET docs = (
            FOR doc IN documents
                {filter_clause}
                SORT doc.created_at DESC
                LIMIT @offset, @limit

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
                COLLECT WITH COUNT INTO total
                RETURN total
        )

        RETURN {{ items: docs, total: FIRST(total_count) || 0 }}
        """

        cursor = self.db.aql.execute(aql, bind_vars=bind_vars)
        result = list(cursor)
        return result[0] if result else {"items": [], "total": 0}

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

    @staticmethod
    def _add_filter_if_present(
        filters: Dict[str, Any],
        key: str,
        aql_filters: List[str],
        bind_vars: Dict[str, Any],
        condition: str,
    ) -> None:
        value = filters.get(key)
        if value is None or value == "":
            return

        aql_filters.append(condition)
        bind_vars[key] = value

    @staticmethod
    def _add_date_filters(
        filters: Dict[str, Any],
        aql_filters: List[str],
        bind_vars: Dict[str, Any],
    ) -> None:
        date_from = filters.get("date_from")
        date_to = filters.get("date_to")

        if isinstance(date_from, date):
            bind_vars["date_from"] = date_from.isoformat()
        elif date_from:
            bind_vars["date_from"] = str(date_from)

        if isinstance(date_to, date):
            bind_vars["date_to"] = f"{date_to.isoformat()}T23:59:59.999999"
        elif date_to:
            bind_vars["date_to"] = str(date_to)

        if "date_from" in bind_vars:
            aql_filters.append("doc.created_at >= @date_from")
        if "date_to" in bind_vars:
            aql_filters.append("doc.created_at <= @date_to")
