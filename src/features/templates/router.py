from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from typing import Optional
from src.core.storage import storage_instance
from src.core.database import db_instance
import uuid
import logging
from datetime import datetime
from minio.commonconfig import CopySource
from typing import List
from fastapi import Query
from .models import RequiredDocumentResponse, PaginatedRequiredDocumentResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/templates", tags=["Templates & Resources"])


def get_db():
    return db_instance.get_db()


@router.get("/", response_model=PaginatedRequiredDocumentResponse)
async def list_required_documents_templates(
        search: Optional[str] = Query(None, description="Buscar por nombre o código del documento"),
        process_id: Optional[str] = Query(None, description="Filtrar por ID del proceso (Ej: Titulación)"),
        only_public: bool = Query(False, description="Si es True, solo muestra documentos marcados como públicos"),
        page: int = Query(1, ge=1, description="Número de página"),
        limit: int = Query(10, ge=1, le=100, description="Registros por página"),
):
    """
    Lista los 'Documentos Requeridos' (Formatos/Plantillas) disponibles.
    Ideal para que el estudiante descargue los formatos vacíos.
    """
    try:
        db = get_db()
        bind_vars = {}
        filters = []
        offset = (page - 1) * limit

        if only_public:
            filters.append("doc.is_public == true")

        if process_id:
            filters.append("doc.process_id == @process_id")
            bind_vars["process_id"] = process_id

        if search:
            # Usamos CONTAINS para búsqueda simple (case-insensitive con LOWER)
            filters.append("""
                (CONTAINS(LOWER(doc.name), LOWER(@search)) OR 
                 CONTAINS(LOWER(doc.code), LOWER(@search)))
            """)
            bind_vars["search"] = search

        # Construir cláusula FILTER
        filter_clause = "FILTER " + " AND ".join(filters) if filters else ""

        # Agregar LIMIT para paginación
        bind_vars["offset"] = offset
        bind_vars["limit"] = limit

        aql = f"""
        FOR doc IN required_documents
            {filter_clause}
            SORT doc.name ASC
            LIMIT @offset, @limit
            RETURN {{
                _key: doc._key,
                name: doc.name,
                code: doc.code,
                description: doc.description,
                is_public: doc.is_public || false,
                process_id: doc.process_id,

                // Info del Template
                has_template: doc.has_template || false,
                template_display_name: doc.template_display_name,
                template_updated_at: DATE_ISO8601(doc.template_updated_at)
            }}
        """

        cursor = db.aql.execute(aql, bind_vars=bind_vars, full_count=True)
        data = list(cursor)
        
        # Obtener total de registros (ignorando el LIMIT)
        total = cursor.statistics().get('fullCount', 0) if cursor.statistics() else len(data)

        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": data
        }

    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo listado de formatos.")


@router.post("/upload")
async def upload_template(
        file: UploadFile = File(...),
        required_document_id: str = Form(...),
        name: Optional[str] = Form(None)
):
    """
    Sube o actualiza un formato/plantilla.
    - Si ya existe uno previo, lo mueve a una carpeta de 'archive' (histórico).
    - Actualiza la entidad en ArangoDB con la nueva ruta y metadatos.
    """
    try:
        logger.info(f"Uploading template for doc: {required_document_id}")
        db = get_db()

        # 1. Obtener documento actual para ver si ya tiene template
        aql_check = """
            FOR doc IN required_documents
            FILTER doc._key == @doc_id
            RETURN doc
        """
        cursor = db.aql.execute(aql_check, bind_vars={"doc_id": required_document_id})
        result = list(cursor)

        if not result:
            raise HTTPException(status_code=404, detail="Documento requerido no encontrado")

        current_doc = result[0]

        # 2. Lógica de Archivado (Versioning)
        # Si ya existe un template_path, lo movemos a 'archive' antes de perder la referencia
        if current_doc.get("template_path"):
            old_path = current_doc["template_path"]
            try:
                # Generamos nombre de archivo para el backup: archive/YYYYMMDD_HHMMSS_uuid.ext
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                old_filename = old_path.split("/")[-1]
                archive_path = f"system-templates/archive/{timestamp}_{old_filename}"

                logger.info(f"Archiving old template to: {archive_path}")

                # MinIO no tiene "move", se hace Copy + (Opcional Delete, pero aquí solo subiremos uno nuevo)
                storage_instance.client.copy_object(
                    storage_instance.bucket_name,
                    archive_path,
                    CopySource(storage_instance.bucket_name, old_path)
                )
                # Opcional: Borrar el viejo de la ruta principal si quieres limpiar,
                # aunque al subir el nuevo simplemente cambiaremos el puntero en la BD.
            except Exception as e:
                logger.warning(f"No se pudo archivar el template anterior: {e}")
                # No bloqueamos el flujo, seguimos con la subida nueva

        # 3. Subir el Nuevo Archivo
        file_ext = file.filename.split(".")[-1]
        new_filename = f"{uuid.uuid4()}.{file_ext}"
        storage_path = f"system-templates/{new_filename}"

        # Resetear puntero del archivo por si acaso
        await file.seek(0)

        storage_instance.client.put_object(
            storage_instance.bucket_name,
            storage_path,
            file.file,
            length=-1,
            part_size=10 * 1024 * 1024,
            content_type=file.content_type
        )

        # 4. Determinar el nombre a mostrar (Display Name)
        # Prioridad:
        # 1. Name enviado en el Form
        # 2. Name que ya tenía el documento (para no sobrescribir nombres personalizados)
        # 3. Nombre original del archivo subido

        final_display_name = name
        if not final_display_name:
            final_display_name = current_doc.get("template_display_name") or file.filename

        # 5. Actualizar ArangoDB (Sincronización)
        aql_update = """
            UPDATE @doc_id WITH {
                template_path: @storage_path,
                template_display_name: @display_name,
                template_original_name: @original_name,
                template_mime_type: @mime_type,
                template_updated_at: DATE_NOW(),
                has_template: true
            } IN required_documents
            RETURN NEW
        """

        update_cursor = db.aql.execute(aql_update, bind_vars={
            "doc_id": required_document_id,
            "storage_path": storage_path,
            "display_name": final_display_name,
            "original_name": file.filename,
            "mime_type": file.content_type
        })

        updated_doc = list(update_cursor)[0]

        return {
            "success": True,
            "message": "Plantilla subida y vinculada exitosamente",
            "data": {
                "id": updated_doc["_key"],
                "template_path": updated_doc["template_path"],
                "display_name": updated_doc["template_display_name"],
                "archived_previous": bool(current_doc.get("template_path"))
            }
        }

    except Exception as e:
        logger.error(f"Error uploading template: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download")
async def get_template_link(path: str):
    return {"url": storage_instance.get_presigned_url(path, expires_in_minutes=120)}