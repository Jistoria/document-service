import logging
import json
from arango.database import StandardDatabase
from arango.exceptions import ArangoError
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

def init_arango_schema(db: StandardDatabase):
    """
    Crea las colecciones y aristas necesarias si no existen.
    """
    print("🛠️ Verificando esquema de ArangoDB...")

    vertex_collections = [
        "entities",
        "meta_schemas",
        "documents",
        "dms_users",
        "required_documents",
        "processes",
        "process_categories",
        "subsystems"
    ]

    edge_collections = [
        "belongs_to",       # entidad -> entidad (estructura)
        "usa_esquema",      # documents -> meta_schemas
        "file_located_in",  # documents -> entidades (ubicación)
        "complies_with",    # documents -> req_doc
        "catalog_belongs_to" # req_doc -> process -> category
    ]

    for col in vertex_collections:
        if not db.has_collection(col):
            db.create_collection(col)
            print(f"    Colección creada: {col}")

    for col in edge_collections:
        if not db.has_collection(col):
            db.create_collection(col, edge=True)
            print(f"    Colección de ARISTAS creada: {col}")

    print("✨ Esquema de base de datos verificado.")


def init_arangosearch_views(db):
    view_name = "entities_search_view"

    # text analyzer (con features que ArangoSearch suele usar)
    name_analyzer = ensure_analyzer(
        db,
        name="text_es",
        analyzer_type="text",
        properties={
            "locale": "es",
            "stemming": True,
            "case": "lower",
            "accent": False,
            "stopwords": [],
        },
        features=["frequency", "position", "norm"],
    )

    type_analyzer = ensure_analyzer(
        db,
        name="norm_es",
        analyzer_type="norm",
        properties={
            "locale": "es.utf-8",
            "case": "lower",
            "accent": False,
        },
        features=[],
    )

    # recrear vista (si quieres)
    try:
        db.delete_view(view_name)
    except Exception:
        pass

    db.create_arangosearch_view(
        name=view_name,
        properties={
            "links": {
                "entities": {
                    "fields": {
                        "name": {"analyzers": [name_analyzer]},
                        "label": {"analyzers": [name_analyzer]},
                        "type": {"analyzers": [type_analyzer, "identity"]},
                        "code": {"analyzers": ["identity"]},
                        "code_numeric": {"analyzers": ["identity"]},
                    }
                },
                "dms_users": {
                    "fields": {
                        "name": {"analyzers": [name_analyzer]},
                        "email": {"analyzers": ["identity"]},
                    }
                },
            }
        },
    )


def _analyzer_signature(a: dict) -> str:
    """Crea una firma estable (ordenada) para comparar analyzers."""
    payload = {
        "type": a.get("type"),
        "properties": a.get("properties", {}),
        "features": a.get("features", []),
    }
    return json.dumps(payload, sort_keys=True)

def ensure_analyzer(db, name: str, analyzer_type: str, properties: dict, features: list[str] | None = None) -> str:
    """Asegura que exista un analyzer. Si hay colisión por definición distinta, crea uno versionado."""
    features = features or []

    desired = {"type": analyzer_type, "properties": properties, "features": features}

    # 1) Buscar si existe
    existing = None
    try:
        for a in db.analyzers():
            if a.get("name") == name:
                existing = a
                break
    except Exception as e:
        logger.warning(f"No se pudo listar analyzers: {e}")

    # 2) Si existe y coincide, usarlo
    if existing:
        if _analyzer_signature(existing) == _analyzer_signature(desired):
            logger.info(f" Analyzer '{name}' ya existe y coincide.")
            return name

        # 3) Si existe pero NO coincide, crea versionado
        versioned = f"{name}_v2"
        logger.warning(
            f"⚠️ Analyzer '{name}' existe pero con definición distinta. "
            f"Usaré '{versioned}'."
        )
        # intenta crear el versionado (si ya existe, úsalo)
        try:
            db.create_analyzer(versioned, analyzer_type=analyzer_type, properties=properties, features=features)
            logger.info(f" Analyzer creado: {versioned}")
        except Exception as e:
            logger.warning(f"⚠️ No pude crear '{versioned}' (quizá ya existe): {e}")
        return versioned

    # 4) Si no existe, créalo normal
    db.create_analyzer(name, analyzer_type=analyzer_type, properties=properties, features=features)
    logger.info(f" Analyzer creado: {name}")
    return name


def configure_minio_cors(client: Minio, bucket_name: str):
    print(f"🔧 Configurando CORS para el bucket: {bucket_name}")

    # Esta política permite que localhost:3000 (tu Vue) lea archivos
    cors_config = {
        "CORSRules": [
            {
                "AllowedHeaders": ["*"],
                "AllowedMethods": ["GET", "HEAD"],
                "AllowedOrigins": ["http://localhost:3000", "http://127.0.0.1:3000"],  # Tu frontend
                "ExposeHeaders": ["ETag"],
                "MaxAgeSeconds": 3000
            }
        ]
    }

    try:
        client.set_bucket_cors(bucket_name, cors_config)
        print(" CORS configurado exitosamente.")
    except S3Error as e:
        print(f"❌ Error configurando CORS: {e}")