from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "Document Service"
    ENV_MODE: str = "dev"
    # ArangoDB
    ARANGO_HOST_URL: str
    ARANGO_ROOT_PASSWORD: str
    ARANGO_DB_NAME: str = "dms_db"

    # MinIO
    MINIO_ENDPOINT: str
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_BUCKET_NAME: str = "documentos"
    MINIO_SECURE: bool = False

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"

    AZURE_TENANT_ID: str = "asd"
    AZURE_CLIENT_ID: str = "id"
    AZURE_CLIENT_SECRET: str = "secret"

    AUTH_REDIS_URL: str = "redis://localhost:6379"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()