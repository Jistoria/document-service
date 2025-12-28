from minio import Minio
from minio.error import S3Error
import os
import io


class StorageService:
    def __init__(self):
        # Usamos variables de entorno o valores por defecto del docker-compose
        self.client = Minio(
            "storage:9000",  # Nombre del servicio en docker-compose
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            secure=False  # True si usas HTTPS
        )
        self.bucket_name = "documents-storage"
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                print(f"🪣 Bucket '{self.bucket_name}' creado exitosamente.")
        except S3Error as e:
            print(f"❌ Error verificando bucket: {e}")

    def upload_file(self, file_data: bytes, destination_path: str, content_type: str):
        """
        Sube bytes a MinIO y retorna la ruta relativa.
        """
        try:
            # Convertir bytes a stream
            data_stream = io.BytesIO(file_data)

            self.client.put_object(
                self.bucket_name,
                destination_path,
                data_stream,
                length=len(file_data),
                content_type=content_type
            )
            # Retornamos formato: bucket/path
            return f"{self.bucket_name}/{destination_path}"
        except S3Error as e:
            print(f"❌ Error subiendo archivo a MinIO: {e}")
            raise e

    def get_presigned_url(self, partial_path: str):
        """
        Genera una URL temporal para ver el archivo (útil para el frontend luego)
        """
        try:
            # Separar bucket y objeto
            parts = partial_path.split("/", 1)
            if len(parts) < 2: return None

            return self.client.get_presigned_url(
                "GET",
                parts[0],  # Bucket
                parts[1]  # Object Path
            )
        except Exception:
            return None


storage_instance = StorageService()