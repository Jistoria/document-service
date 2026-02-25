from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from minio.error import S3Error

from src.core.security.auth import AuthContext, get_auth_context
from src.core.storage import storage_instance

from .service import storage_proxy_service

router = APIRouter(prefix="/storage", tags=["Storage Proxy"])


@router.get("/proxy/{object_path:path}")
async def get_file_via_proxy(
    object_path: str,
    request: Request,
    background_tasks: BackgroundTasks,
    ctx: AuthContext = Depends(get_auth_context),
):
    """Proxy de descarga con autorización ABAC y auditoría asíncrona."""
    try:
        document = await storage_proxy_service.authorize_document_download(object_path, ctx)

        clean_path = storage_proxy_service.normalize_object_path(object_path)
        data_stream = storage_instance.client.get_object(storage_instance.bucket_name, clean_path)

        media_type = "application/octet-stream"
        if clean_path.endswith(".pdf"):
            media_type = "application/pdf"
        elif clean_path.endswith(".png"):
            media_type = "image/png"
        elif clean_path.endswith(".jpg") or clean_path.endswith(".jpeg"):
            media_type = "image/jpeg"
        elif clean_path.endswith(".json"):
            media_type = "application/json"

        doc_id = document.get("_key", "")
        ip_address = request.client.host if request.client else None

        background_tasks.add_task(
            storage_proxy_service.log_document_download,
            doc_id,
            ctx.user_id,
            ip_address,
        )
        background_tasks.add_task(data_stream.close)

        return StreamingResponse(
            data_stream,
            media_type=media_type,
            headers={
                "Content-Disposition": f"inline; filename={clean_path.split('/')[-1]}"
            },
        )

    except HTTPException:
        raise
    except S3Error as e:
        if e.code == "NoSuchKey":
            raise HTTPException(status_code=404, detail="Archivo no encontrado.")
        raise HTTPException(status_code=500, detail=f"Error de Storage: {str(e)}")
    except Exception as e:
        print(f"Error proxying file: {e}")
        raise HTTPException(status_code=500, detail="Error interno al procesar el archivo.")
