from typing import List, Dict, Any
from src.core.database import db_instance
from .models import CatalogItem, CareerItem

class CatalogService:
    def get_db(self):
        return db_instance.get_db()

    async def get_faculties(self) -> List[CatalogItem]:
        db = self.get_db()
        aql = """
        FOR doc IN entities
            FILTER doc.type == 'facultad'
            SORT doc.name ASC
            RETURN {
                id: doc._key,
                name: doc.name,
                code: doc.code
            }
        """
        cursor = db.aql.execute(aql)
        return [CatalogItem(**doc) for doc in cursor]

    async def get_careers(self, faculty_id: str = None) -> List[CareerItem]:
        db = self.get_db()
        
        # Si se especifica facultad, filtramos por la relación belongs_to
        # Carrera -> Facultad (child -> parent)
        if faculty_id:
            aql = """
            FOR fac IN entities
                FILTER fac._key == @faculty_id AND fac.type == 'facultad'
                FOR car, edge IN INBOUND fac belongs_to
                    FILTER car.type == 'carrera'
                    SORT car.name ASC
                    RETURN {
                        id: car._key,
                        name: car.name,
                        code: car.code,
                        faculty_id: fac._key,
                        faculty_name: fac.name
                    }
            """
            bind_vars = {"faculty_id": faculty_id}
        else:
            # Todas las carreras, opcionalmente uniendo con su facultad si es necesario
            aql = """
            FOR car IN entities
                FILTER car.type == 'carrera'
                // Intentamos buscar su facultad (padre)
                LET parents = (
                    FOR fac IN OUTBOUND car belongs_to
                    FILTER fac.type == 'facultad'
                    RETURN fac
                )
                LET fac = LENGTH(parents) > 0 ? parents[0] : null
                
                SORT car.name ASC
                RETURN {
                    id: car._key,
                    name: car.name,
                    code: car.code,
                    faculty_id: fac._key,
                    faculty_name: fac.name
                }
            """
            bind_vars = {}

        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        return [CareerItem(**doc) for doc in cursor]

    async def get_process_tree(self) -> List[Dict[str, Any]]:
        """
        Retorna la estructura jerárquica de procesos para menús o selectores en cascada.
        Subsystems -> Categories -> Processes
        """
        db = self.get_db()
        # Verificar que existan las colecciones antes de consultar (por si no se ha sincronizado)
        if not db.has_collection("subsystems"):
            return []

        aql = """
        FOR sub IN subsystems
            SORT sub.name ASC
            LET categories = (
                FOR cat IN INBOUND sub catalog_belongs_to
                    SORT cat.name ASC
                    LET processes = (
                        FOR proc IN INBOUND cat catalog_belongs_to
                            SORT proc.name ASC
                            RETURN {
                                id: proc._key,
                                name: proc.name,
                                code: proc.code,
                                type: "process"
                            }
                    )
                    RETURN {
                        id: cat._key,
                        name: cat.name,
                        code: cat.code,
                        type: "category",
                        children: processes
                    }
            )
            RETURN {
                id: sub._key,
                name: sub.name,
                code: sub.code,
                type: "subsystem",
                children: categories
            }
        """
        cursor = db.aql.execute(aql)
        return list(cursor)

    async def get_required_documents(self, process_id: str) -> List[CatalogItem]:
        db = self.get_db()
        if not db.has_collection("required_documents"):
            return []
            
        aql = """
        FOR proc IN processes
            FILTER proc._key == @process_id
            FOR doc IN INBOUND proc catalog_belongs_to
                // Filtrar solo nodos de required_documents (aunque la colección del edge apunta, validamos)
                // En catalog_belongs_to: doc -> proc
                FILTER IS_SAME_COLLECTION('required_documents', doc)
                SORT doc.name ASC
                RETURN {
                    id: doc._key,
                    name: doc.name,
                    code: doc.code
                }
        """
        cursor = db.aql.execute(aql, bind_vars={"process_id": process_id})
        return [CatalogItem(**doc) for doc in cursor]

catalog_service = CatalogService()
