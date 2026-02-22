from typing import List, Dict, Any, Optional
from src.core.database import db_instance
from .models import (
    CatalogItem, CareerItem, SubsystemItem, CategoryItem, 
    ProcessItem, ProcessDetail, RequiredDocumentItem
)
from .repository import catalog_repository


class CatalogService:
    """
    Servicio de catálogo que coordina la lógica de negocio.
    Delega las queries al repository para mejor separación de responsabilidades.
    """
    
    def get_db(self):
        return db_instance.get_db()

    # ========== ESTRUCTURA ORGANIZACIONAL ==========

    async def get_faculties(self, search: Optional[str] = None) -> List[CatalogItem]:
        """Obtiene lista de facultades con búsqueda opcional"""
        db = self.get_db()
        aql, bind_vars = catalog_repository.get_faculties_query(search)
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        return [CatalogItem(**doc) for doc in cursor]

    async def get_careers(self, faculty_id: Optional[str] = None, 
                         search: Optional[str] = None) -> List[CareerItem]:
        """Obtiene lista de carreras con búsqueda opcional"""
        db = self.get_db()
        aql, bind_vars = catalog_repository.get_careers_query(faculty_id, search)
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        return [CareerItem(**doc) for doc in cursor]

    # ========== CATÁLOGO DE PROCESOS ==========

    async def get_subsystems(self, search: Optional[str] = None) -> List[SubsystemItem]:
        """Obtiene lista de subsistemas con búsqueda opcional"""
        db = self.get_db()
        if not db.has_collection("subsystems"):
            return []
        
        aql, bind_vars = catalog_repository.get_subsystems_query(search)
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        return [SubsystemItem(**doc) for doc in cursor]

    async def get_categories(self, subsystem_id: Optional[str] = None, 
                           search: Optional[str] = None) -> List[CategoryItem]:
        """Obtiene categorías con búsqueda opcional"""
        db = self.get_db()
        if not db.has_collection("process_categories"):
            return []
        
        aql, bind_vars = catalog_repository.get_categories_query(subsystem_id, search)
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        return [CategoryItem(**doc) for doc in cursor]

    async def get_processes(self, category_id: Optional[str] = None, 
                           parent_process_id: Optional[str] = None,
                           search: Optional[str] = None) -> List[ProcessItem]:
        """Obtiene procesos con búsqueda opcional"""
        db = self.get_db()
        if not db.has_collection("processes"):
            return []
        
        aql, bind_vars = catalog_repository.get_processes_query(category_id, parent_process_id, search)
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        return [ProcessItem(**doc) for doc in cursor]

    async def get_process_detail(self, process_id: str) -> Optional[ProcessDetail]:
        """Obtiene detalle completo de un proceso con subprocesos y documentos"""
        db = self.get_db()
        if not db.has_collection("processes"):
            return None
        
        # Obtener proceso principal
        aql, bind_vars = catalog_repository.get_process_detail_query(process_id)
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        results = list(cursor)
        if not results:
            return None
        
        process_data = results[0]
        
        # Construir recursivamente
        return self._build_process_detail(db, process_id, process_data)

    async def get_required_documents(self, process_id: Optional[str] = None, 
                                    subsystem_id: Optional[str] = None,
                                    search: Optional[str] = None) -> List[RequiredDocumentItem]:
        """Obtiene documentos requeridos con búsqueda opcional"""
        db = self.get_db()
        if not db.has_collection("required_documents"):
            return []
        
        aql, bind_vars = catalog_repository.get_required_documents_query(process_id, subsystem_id, search)
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        return [RequiredDocumentItem(**doc) for doc in cursor]

    # ========== PROCESS TREE ==========

    async def get_process_tree(self) -> List[Dict[str, Any]]:
        """
        Retorna estructura jerárquica completa de procesos con subprocesos.
        Subsystems -> Categories -> Processes -> Subprocesses (recursivo)
        """
        db = self.get_db()
        if not db.has_collection("subsystems"):
            return []

        # Obtener estructura base
        aql, bind_vars = catalog_repository.get_process_tree_base_query()
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        tree = list(cursor)
        
        # Agregar subprocesos recursivamente
        for subsystem in tree:
            for category in subsystem.get('children', []):
                for i, process in enumerate(category.get('children', [])):
                    category['children'][i] = self._build_process_with_subprocesses(db, process)
        
        return tree

    # ========== HELPERS RECURSIVOS ==========

    def _build_process_with_subprocesses(self, db, process_data: Dict) -> Dict:
        """Helper recursivo para construir árbol de subprocesos"""
        process_id = process_data.get('_key') or process_data.get('id')
        
        # Obtener subprocesos
        aql, bind_vars = catalog_repository.get_subprocesses_query(process_id)
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        subprocesses = list(cursor)
        
        result = {
            "id": process_id,
            "name": process_data.get('name'),
            "code": process_data.get('code'),
            "type": "process",
            "children": []
        }
        
        # Recursión
        for sub in subprocesses:
            result["children"].append(self._build_process_with_subprocesses(db, sub))
        
        return result

    def _build_process_detail(self, db, process_id: str, process_data: Dict) -> ProcessDetail:
        """Helper recursivo para construir detalle completo de proceso"""
        # Obtener subprocesos
        aql_subs, bind_vars_subs = catalog_repository.get_subprocesses_query(process_id)
        cursor_subs = db.aql.execute(aql_subs, bind_vars=bind_vars_subs)
        subprocesses_data = list(cursor_subs)
        
        # Obtener documentos requeridos
        aql_docs, bind_vars_docs = catalog_repository.get_process_documents_query(process_id)
        cursor_docs = db.aql.execute(aql_docs, bind_vars=bind_vars_docs)
        docs_data = list(cursor_docs)
        
        # Recursión en subprocesos
        subprocesses = []
        for sub_data in subprocesses_data:
            subprocesses.append(self._build_process_detail(db, sub_data['id'], sub_data))
        
        required_docs = [RequiredDocumentItem(**doc) for doc in docs_data]
        
        return ProcessDetail(
            **process_data,
            has_subprocesses=len(subprocesses) > 0,
            subprocesses=subprocesses,
            required_documents=required_docs
        )


catalog_service = CatalogService()
