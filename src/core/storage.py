from minio import Minio
from minio.error import S3Error
import os
import io
from datetime import timedelta

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
            print(f"Error verificando bucket: {e}")

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
            print(f"Error subiendo archivo a MinIO: {e}")
            raise e

    def get_presigned_url(self, object_path: str, expires_in_minutes: int = 60) -> str:
        # 1. CORRECCIÓN DE RUTA: Quitamos el bucket si viene en el path
        # Si path es "documents-storage/stage/file.pdf", lo dejamos en "stage/file.pdf"
        clean_object_path = object_path.replace(f"{self.bucket_name}/", "", 1)

        try:
            # 2. GENERACIÓN DE URL
            # Nota: Minio firma usando el endpoint configurado en 'self.client'.
            # Si self.client apunta a "minio:9000", la URL saldrá con "minio:9000".
            url = self.client.presigned_get_object(
                bucket_name=self.bucket_name,
                object_name=clean_object_path,
                expires=timedelta(minutes=expires_in_minutes)
            )

            return url

        except Exception as e:
            print(f"Error generando URL firmada: {e}")
            return ""


storage_instance = StorageService()