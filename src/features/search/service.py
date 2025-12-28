import math
from src.core.database import db_instance
# Asegúrate de importar las clases originales. 
# No necesitamos importar los Wrappers aquí si devolvemos diccionarios, 
# pero sí necesitamos los modelos de datos internos (DocumentDetail, etc).
from .models import DocumentDetail, DocumentListResponse, EntityRef, DetailPagination

class SearchService:

    def get_db(self):
        return db_instance.get_db()

    def get_document_by_id(self, doc_id: str):
        db = self.get_db()
        # Query AQL normal
        aql = """
        FOR doc IN documents
            FILTER doc._key == @doc_id

            LET entity = (
                FOR v IN 1..1 OUTBOUND doc belongs_to
                RETURN { id: v._key, name: v.name, type: v.type, code: v.code }
            )[0]

            LET schema = (
                FOR v IN 1..1 OUTBOUND doc usa_esquema
                RETURN { id: v._key, name: v.name, version: v.version }
            )[0]

            RETURN MERGE(doc, { 
                context_entity: entity, 
                used_schema: schema 
            })
        """
        cursor = db.aql.execute(aql, bind_vars={"doc_id": doc_id})
        result = list(cursor)

        # --- FIX: Formato API Standard ---
        if result:
            return {
                "success": True,
                "data": DocumentDetail(**result[0]),
                "message": "Documento encontrado exitosamente."
            }
        else:
            return {
                "success": False,
                "data": None,
                "message": "El documento no existe o no fue encontrado."
            }

    def search_documents(self, page: int = 1, page_size: int = 10, entity_id: str = None, status: str = None):
        db = self.get_db()
        offset = (page - 1) * page_size

        bind_vars = {
            "offset": offset,
            "limit": page_size
        }

        # --- Filtros Dinámicos ---
        filters = []

        if status:
            filters.append("doc.status == @status")
            bind_vars["status"] = status

        if entity_id:
            filters.append("""
                (FOR v IN 1..1 OUTBOUND doc belongs_to FILTER v._key == @entity_id RETURN 1)[0] == 1
            """)
            bind_vars["entity_id"] = entity_id

        filter_clause = "FILTER " + " AND ".join(filters) if filters else ""

        # --- QUERY AQL ---
        aql = f"""
        LET docs = (
            FOR doc IN documents
                {filter_clause}
                SORT doc.created_at DESC
                LIMIT @offset, @limit

                LET entity = (FOR v IN 1..1 OUTBOUND doc belongs_to RETURN {{ id: v._key, name: v.name, type: v.type }})[0]
                LET schema = (FOR v IN 1..1 OUTBOUND doc usa_esquema RETURN {{ id: v._key, name: v.name }})[0]

                RETURN MERGE(doc, {{ context_entity: entity, used_schema: schema }})
        )

        LET total_count = (
            FOR doc IN documents
            {filter_clause}
            RETURN 1
        )

        RETURN {{ items: docs, total: LENGTH(total_count) }}
        """

        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        data = list(cursor)[0]

        # --- PROCESAMIENTO DE DATOS ---
        items_list = [DocumentDetail(**d) for d in data["items"]]
        total_items = data["total"]

        # --- CÁLCULOS DE PAGINACIÓN ---
        if total_items > 0:
            last_page = math.ceil(total_items / page_size)
        else:
            last_page = 1

        to_item = offset + len(items_list)
        has_more = page < last_page

        # Objeto interno de datos
        internal_data = DocumentListResponse(
            data=items_list,
            pagination=DetailPagination(
                currentPage=page,
                lastPage=last_page,
                perPage=page_size,
                total=total_items,
                to=to_item,
                hasMorePages=has_more
            )
        )

        # --- FIX: Formato API Standard ---
        return {
            "success": True,
            "data": internal_data,
            "message": "Búsqueda completada exitosamente."
        }


    def get_available_entities(self):
        db = self.get_db()
        aql = """
        FOR doc IN documents
            FOR entity IN 1..1 OUTBOUND doc belongs_to
            RETURN DISTINCT {
                id: entity._key,
                name: entity.name,
                type: entity.type
            }
        """
        cursor = db.aql.execute(aql)
        entities = [EntityRef(**d) for d in cursor]

        # --- FIX: Formato API Standard ---
        return {
            "success": True,
            "data": entities,
            "message": f"Se encontraron {len(entities)} entities con documentos."
        }

search_service = SearchService()
