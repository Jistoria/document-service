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

    # DMS
    DMS_MICROSERVICE_ID: str = "a0f43301-466e-422c-85cf-061947508721"

    AUTH_JWKS_URL: str = "https://auth.example.com/.well-known/jwks.json"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()