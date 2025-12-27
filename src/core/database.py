from arango import ArangoClient
from src.core.config import settings


class Database:
    def __init__(self):
        self.client = ArangoClient(hosts=settings.ARANGO_HOST_URL)

    def get_db(self):
        # Conectarse como root para verificar/crear la DB
        sys_db = self.client.db("_system", username="root", password=settings.ARANGO_ROOT_PASSWORD)

        if not sys_db.has_database(settings.ARANGO_DB_NAME):
            sys_db.create_database(settings.ARANGO_DB_NAME)
            print(f"✅ Base de datos '{settings.ARANGO_DB_NAME}' creada.")

        # Retornar conexión a la DB específica
        return self.client.db(
            settings.ARANGO_DB_NAME,
            username="root",
            password=settings.ARANGO_ROOT_PASSWORD
        )


# Instancia global
db_instance = Database()


def get_db():
    """Dependencia para inyectar en los endpoints"""
    return db_instance.get_db()