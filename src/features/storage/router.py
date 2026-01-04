from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import StreamingResponse
from minio.error import S3Error
# Asumo que importas tu instancia de servicio ya configurada
from src.core.storage import storage_instance

router = APIRouter(prefix="/storage", tags=["Storage Proxy"])


@router.get("/proxy/{object_path:path}")
async def get_file_via_proxy(object_path: str):
    """
    Proxy de descarga: El Frontend pide aquí, el Backend pide a MinIO
    y devuelve los bytes en streaming.

    Ventaja: Evita problemas de CORS y redes Docker.
    """
    try:
        # 1. Obtenemos el objeto de MinIO (Stream)
        # Usamos el cliente interno (el que sí conecta dentro de Docker)
        # Nota: Asegúrate de limpiar el nombre del bucket si viene en el path
        clean_path = object_path
        if object_path.startswith(f"{storage_instance.bucket_name}/"):
            clean_path = object_path.replace(f"{storage_instance.bucket_name}/", "", 1)

        # MinIO get_object devuelve un objeto response tipo stream
        data_stream = storage_instance.client.get_object(
            storage_instance.bucket_name,
            clean_path
        )

        # 2. Determinamos el Content-Type correcto
        # Si es PDF, navegador lo muestra. Si es desconocido, descarga.
        media_type = "application/octet-stream"
        if clean_path.endswith(".pdf"):
            media_type = "application/pdf"
        elif clean_path.endswith(".png"):
            media_type = "image/png"
        elif clean_path.endswith(".jpg") or clean_path.endswith(".jpeg"):
            media_type = "image/jpeg"
        elif clean_path.endswith(".json"):
            media_type = "application/json"

        # 3. Retornamos el StreamingResponse
        # Esto conecta el tubo de MinIO directo al tubo del Cliente
        return StreamingResponse(
            data_stream,
            media_type=media_type,
            headers={
                # "inline" hace que el navegador intente mostrarlo (visor PDF)
                # "attachment" forzaría la descarga
                "Content-Disposition": f"inline; filename={clean_path.split('/')[-1]}"
            }
        )

    except S3Error as e:
        if e.code == "NoSuchKey":
            raise HTTPException(status_code=404, detail="Archivo no encontrado.")
        raise HTTPException(status_code=500, detail=f"Error de Storage: {str(e)}")

    except Exception as e:
        print(f"Error proxying file: {e}")
        raise HTTPException(status_code=500, detail="Error interno al procesar el archivo.")