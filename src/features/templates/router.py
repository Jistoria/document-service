from fastapi import APIRouter, UploadFile, File, HTTPException
from src.core.storage import storage_instance
import shutil
import uuid

router = APIRouter(prefix="/templates", tags=["Templates & Resources"])


@router.post("/upload")
async def upload_template(file: UploadFile = File(...)):
    """
    Sube un formato/plantilla al bucket de sistema.
    Retorna el path para que el Management Service lo guarde.
    """
    try:
        # 1. Generar una ruta limpia
        # Usamos una carpeta separada 'system-templates' para no mezclar con documentos de usuarios
        file_ext = file.filename.split(".")[-1]
        new_filename = f"{uuid.uuid4()}.{file_ext}"
        storage_path = f"system-templates/{new_filename}"

        # 2. Subir a MinIO (Usando tu cliente interno)
        # Nota: storage_service debe tener lógica para put_object con stream
        storage_instance.client.put_object(
            storage_instance.bucket_name,
            storage_path,
            file.file,
            length=-1,
            part_size=10 * 1024 * 1024
        )

        # 3. Retornar la referencia
        return {
            "success": True,
            "data": {
                "storage_path": storage_path,
                "original_name": file.filename,
                "mime_type": file.content_type
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download")
async def get_template_link(path: str):
    """
    Genera un link público o firmado para descargar la plantilla.
    """
    # Reutilizamos tu lógica de URLs firmadas
    return {"url": storage_instance.get_presigned_url(path, expires_in_minutes=120)}