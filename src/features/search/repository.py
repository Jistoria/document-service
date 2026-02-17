from datetime import date
from typing import Any, Dict, List, Optional, Tuple

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
                required_document: req_doc,
                has_integrity_signature: HAS(doc, 'integrity') AND doc.integrity != null AND doc.integrity.manifest_signature != null,
                has_custom_display_name: HAS(doc, 'snap_context_name') AND doc.snap_context_name != null AND doc.snap_context_name != COALESCE(doc.display_name, doc.naming.display_name)
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
            "process_ids",
            aql_filters,
            bind_vars,
            """
            LENGTH(
                FOR node IN 1..6 OUTBOUND doc complies_with, catalog_belongs_to
                FILTER node._key IN @process_ids
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
        self._add_metadata_filters(filters, aql_filters, bind_vars)

        filter_clause = f"FILTER {' AND '.join(aql_filters)}" if aql_filters else ""
        search_clause, search_sort_clause, source, bind_vars = self._build_search_clause(filters, bind_vars)

        aql = f"""
        LET docs = (
            FOR doc IN {source}
                {search_clause}
                {filter_clause}
                {search_sort_clause}
                LIMIT @offset, @limit

                LET entity = (FOR v IN 1..1 OUTBOUND doc file_located_in RETURN {{ id: v._key, name: v.name, type: v.type }})[0]
                LET schema = (FOR v IN 1..1 OUTBOUND doc usa_esquema RETURN {{ id: v._key, name: v.name }})[0]
                LET req_doc = (FOR v IN 1..1 OUTBOUND doc complies_with RETURN {{ id: v._key, name: v.name, code_default: v.code }})[0]

                RETURN MERGE(doc, {{
                    context_entity: entity,
                    used_schema: schema,
                    required_document: req_doc,
                    has_integrity_signature: HAS(doc, 'integrity') AND doc.integrity != null AND doc.integrity.manifest_signature != null,
                    has_custom_display_name: HAS(doc, 'snap_context_name') AND doc.snap_context_name != null AND doc.snap_context_name != COALESCE(doc.display_name, doc.naming.display_name)
                }})
        )

        LET total_count = (
            FOR doc IN {source}
                {search_clause}
                {filter_clause}
                COLLECT WITH COUNT INTO total
                RETURN total
        )

        RETURN {{ items: docs, total: FIRST(total_count) || 0 }}
        """

        cursor = self.db.aql.execute(aql, bind_vars=bind_vars)
        result = list(cursor)
        return result[0] if result else {"items": [], "total": 0}


    def get_metadata_filter_catalog(self, required_document_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene esquema y campos de metadatos para pintar filtros de búsqueda."""
        aql = """
        LET required_doc = FIRST(
            FOR req IN required_documents
                FILTER req._key == @required_document_id
                RETURN { id: req._key, name: req.name, code_default: req.code }
        )

        LET schema = FIRST(
            FOR req IN required_documents
                FILTER req._key == @required_document_id
                FOR s IN 1..1 OUTBOUND req usa_esquema
                RETURN { id: s._key, name: s.name, version: s.version, fields: s.fields }
        )

        LET schema_from_attr = (
            FOR req IN required_documents
                FILTER req._key == @required_document_id
                FILTER req.schema_id != null
                FOR s IN meta_schemas
                    FILTER s._key == req.schema_id
                    LIMIT 1
                    RETURN { id: s._key, name: s.name, version: s.version, fields: s.fields }
        )

        LET resolved_schema = schema != null ? schema : FIRST(schema_from_attr)

        RETURN {
            required_document: required_doc,
            schema: resolved_schema,
            metadata_fields: (
                FOR f IN (resolved_schema != null ? (resolved_schema.fields || []) : [])
                    SORT TO_NUMBER(f.sortOrder) ASC, f.label ASC
                    RETURN {
                        key: f.fieldKey,
                        label: f.label,
                        data_type: f.dataType,
                        input_type: f.typeInput != null ? f.typeInput.key : null,
                        entity_type: f.entityType != null ? f.entityType.key : null,
                        required: TO_BOOL(f.isRequired),
                        sort_order: TO_NUMBER(f.sortOrder)
                    }
            )
        }
        """
        cursor = self.db.aql.execute(aql, bind_vars={"required_document_id": required_document_id})
        result = list(cursor)
        payload = result[0] if result else None
        if not payload or not payload.get("required_document"):
            return None

        if not payload.get("schema"):
            payload["schema"] = {"id": "", "name": "Sin esquema", "version": None}
            payload["metadata_fields"] = []

        return payload

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


    def _build_search_clause(
        self,
        filters: Dict[str, Any],
        bind_vars: Dict[str, Any],
    ) -> Tuple[str, str, str, Dict[str, Any]]:
        search = filters.get("search")
        if not search:
            return "", "SORT doc.created_at DESC", "documents", bind_vars

        bind_vars["search"] = search
        search_clause = """
        SEARCH ANALYZER(
            PHRASE(doc.naming.display_name, @search)
            OR doc.naming.display_name IN TOKENS(@search, "text_es")
            OR doc.original_filename IN TOKENS(@search, "text_es"),
            "text_es"
        )
        """

        return search_clause, "SORT BM25(doc) DESC, doc.created_at DESC", "documents_search_view", bind_vars

    @staticmethod
    def _metadata_value_expr(key_bind: str) -> str:
        """Expresión tolerante para metadatos normalizados y legacy."""
        return (
            "COALESCE("
            f"doc.validated_metadata[@{key_bind}].value, "
            f"doc.validated_metadata[@{key_bind}].display_name, "
            f"doc.validated_metadata[@{key_bind}].name, "
            f"doc.validated_metadata[@{key_bind}], "
            "'')"
        )

    @staticmethod
    def _metadata_string_distance(value: str) -> int:
        """Fuzziness moderado para no impactar drásticamente precisión."""
        normalized_length = len((value or "").strip())
        if normalized_length <= 6:
            return 1
        if normalized_length <= 16:
            return 2
        return 3

    @classmethod
    def _add_metadata_filters(
        cls,
        filters: Dict[str, Any],
        aql_filters: List[str],
        bind_vars: Dict[str, Any],
    ) -> None:
        metadata_filters = filters.get("metadata_filters")
        if not isinstance(metadata_filters, dict) or not metadata_filters:
            return

        for index, (clave, valor) in enumerate(metadata_filters.items()):
            key_bind = f"meta_key_{index}"
            bind_vars[key_bind] = clave
            metadata_value_expr = cls._metadata_value_expr(key_bind)

            if isinstance(valor, dict):
                gte = valor.get("gte")
                lte = valor.get("lte")

                if gte is not None:
                    gte_bind = f"meta_gte_{index}"
                    bind_vars[gte_bind] = gte
                    aql_filters.append(f"{metadata_value_expr} >= @{gte_bind}")

                if lte is not None:
                    lte_bind = f"meta_lte_{index}"
                    bind_vars[lte_bind] = lte
                    aql_filters.append(f"{metadata_value_expr} <= @{lte_bind}")

                continue

            value_bind = f"meta_value_{index}"
            bind_vars[value_bind] = valor

            if isinstance(valor, str):
                distance_bind = f"meta_distance_{index}"
                bind_vars[distance_bind] = cls._metadata_string_distance(valor)
                aql_filters.append(
                    "(" 
                    f"CONTAINS(LOWER(TO_STRING({metadata_value_expr})), LOWER(@{value_bind})) "
                    f"OR LEVENSHTEIN_DISTANCE(LOWER(TO_STRING({metadata_value_expr})), LOWER(@{value_bind})) <= @{distance_bind}"
                    ")"
                )
                continue

            aql_filters.append(f"{metadata_value_expr} == @{value_bind}")

    @staticmethod
    def _add_filter_if_present(
        filters: Dict[str, Any],
        key: str,
        aql_filters: List[str],
        bind_vars: Dict[str, Any],
        condition: str,
    ) -> None:
        value = filters.get(key)
        if value is None or value == "" or value == []:
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
