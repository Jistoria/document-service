from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from minio.error import S3Error

from src.core.database import get_db
from src.core.security.auth import AuthContext, get_auth_context
from src.core.security.permissions import get_permitted_scopes_logic

from .service import (
    build_streaming_payload,
    get_document_snapshot,
    log_document_download,
    validate_download_permissions,
)

router = APIRouter(prefix="/storage", tags=["Storage Proxy"])


@router.get("/proxy/{object_path:path}")
async def get_file_via_proxy(
    object_path: str,
    request: Request,
    background_tasks: BackgroundTasks,
    doc_id: str = Query(..., description="ID del documento asociado al archivo a descargar"),
    ctx: AuthContext = Depends(get_auth_context),
    db=Depends(get_db),
):
    """
    Proxy de descarga autenticado: valida permisos ABAC + equipos y luego
    retorna los bytes desde MinIO en streaming.
    """
    try:
        doc_snapshot = get_document_snapshot(db, doc_id)
        if not doc_snapshot:
            raise HTTPException(status_code=404, detail="Documento no encontrado")

        read_teams = await get_permitted_scopes_logic("dms.document.read", ctx)
        validate_download_permissions(
            db=db,
            doc_snapshot=doc_snapshot,
            user_id=ctx.user_id,
            read_teams=read_teams,
        )

        data_stream, media_type, filename = build_streaming_payload(doc_snapshot, object_path)

        background_tasks.add_task(
            log_document_download,
            db,
            doc_id,
            ctx.user_id,
            request.client.host if request.client else None,
        )

        return StreamingResponse(
            data_stream,
            media_type=media_type,
            headers={"Content-Disposition": f"inline; filename={filename}"},
        )

    except S3Error as e:
        if e.code == "NoSuchKey":
            raise HTTPException(status_code=404, detail="Archivo no encontrado.") from e
        raise HTTPException(status_code=500, detail=f"Error de Storage: {str(e)}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error interno al procesar el archivo.") from e
