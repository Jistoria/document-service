from typing import List, Optional
from pydantic import BaseModel

class CatalogItem(BaseModel):
    id: str  # _key
    name: str
    code: Optional[str] = None
    
class CareerItem(CatalogItem):
    faculty_id: Optional[str] = None
    faculty_name: Optional[str] = None

class HierarchyNode(BaseModel):
    id: str
    name: str
    type: str
    children: List['HierarchyNode'] = []

class ProcessItem(CatalogItem):
    pass
