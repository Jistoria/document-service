from pydantic import BaseModel, Field
from typing import Optional, List


class RequiredDocumentResponse(BaseModel):
    id: str = Field(alias="_key")
    name: str
    code: str
    description: Optional[str] = None

    # Datos del Template
    has_template: bool = False
    template_display_name: Optional[str] = None
    template_updated_at: Optional[str] = None
    template_path: Optional[str] = None

    # Metadata extra
    is_public: bool = False
    process_id: Optional[str] = None

    class Config:
        populate_by_name = True


class PaginatedRequiredDocumentResponse(BaseModel):
    total: int
    page: int
    limit: int
    data: List[RequiredDocumentResponse]