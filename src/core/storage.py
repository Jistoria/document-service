from minio import Minio
from src.core.config import settings


class Storage:
    def __init__(self):
        # MinIO espera el endpoint sin http:// a veces, dependiendo de la versión,
        # pero la librería suele manejarlo. Si falla, quita 'http://' del .env
        endpoint = settings.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")

        self.client = Minio(
            endpoint,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=settings.MINIO_SECURE
        )

        # Asegurar que el bucket existe
        if not self.client.bucket_exists(settings.MINIO_BUCKET_NAME):
            self.client.make_bucket(settings.MINIO_BUCKET_NAME)
            print(f"✅ Bucket '{settings.MINIO_BUCKET_NAME}' creado.")


storage_instance = Storage()


def get_storage():
    return storage_instance.client