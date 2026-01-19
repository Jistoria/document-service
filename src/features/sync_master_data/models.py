from pydantic import BaseModel, Field
from typing import List, Optional, Any

# --- 1. Modelos de Estructura Organizacional ---

class CareerSync(BaseModel):
    id: str
    name: str
    code: Optional[str] = None
    code_numeric: Optional[str] = None

class DepartmentSync(BaseModel):
    id: str
    name: str
    code: Optional[str] = None
    code_numeric: Optional[str] = None
    careers: List[CareerSync] = []

class HeadOfficeSync(BaseModel):
    id: str
    name: str
    code: Optional[str] = None
    code_numeric: Optional[str] = None
    # Mapeamos el campo JSON 'departments' a este modelo
    departments: List[DepartmentSync] = []

# --- 2. Modelos de Esquemas de Metadatos ---

class EntityTypeSync(BaseModel):
    id: int
    key: str
    label: str

class TypeInputSync(BaseModel):
    id: int
    key: str
    label: str

class SchemaFieldSync(BaseModel):
    id: str
    fieldKey: str
    label: str
    isRequired: bool
    sortOrder: int
    dataType: str
    # Estos son opcionales porque "Periodo académico" los tiene en null
    typeInput: Optional[TypeInputSync] = None
    entityType: Optional[EntityTypeSync] = None

class MetadataSchemaSync(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    version: int
    # El JSON usa 'metadataFields'
    metadataFields: List[SchemaFieldSync] = []

# --- 3. NUEVOS: Modelos de Catálogo de Procesos ---

class RequiredDocumentSync(BaseModel):
    id: str
    name: str
    codeDefault: str
    isPublic: bool
    processId: str
    metadataSchemaId: Optional[str] = None

class ProcessSync(BaseModel):
    id: str
    name: str
    code: str
    parentId: Optional[str] = None
    # Recursividad: Un proceso puede tener subprocesos
    subProcesses: List['ProcessSync'] = []
    requiredDocuments: List[RequiredDocumentSync] = []

class ProcessCategorySync(BaseModel):
    id: str
    name: str
    code: str
    processes: List[ProcessSync] = []

class SubsystemSync(BaseModel):
    id: str
    name: str
    code: str
    processCategories: List[ProcessCategorySync] = []

# --- 4. Modelo Principal (Root) ---
class MasterDataExport(BaseModel):
    structure: List[HeadOfficeSync]
    schemas: List[MetadataSchemaSync]
    catalog: List[SubsystemSync] = [] # <--- NUEVO CAMPO