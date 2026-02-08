from typing import Generic, TypeVar, List, Optional
from pydantic import BaseModel

T = TypeVar('T')

class PaginationMetadata(BaseModel):
    total: int
    page: int
    limit: int
    total_pages: int

class RequestSuccessData(BaseModel, Generic[T]):
    count: Optional[int] = None
    data: T
    pagination: Optional[PaginationMetadata] = None

class StandardResponse(BaseModel, Generic[T]):
    success: bool
    message: str
    data: RequestSuccessData[T]

def wrap_response(data: T, message: str = "Petición exitosa", count: int = -1, pagination: Optional[dict] = None) -> StandardResponse[T]:
    if count == -1 and isinstance(data, list):
        count = len(data)
    
    meta = None
    if pagination:
        meta = PaginationMetadata(**pagination)

    return StandardResponse(
        success=True,
        message=message,
        data=RequestSuccessData(
            count=count if count >= 0 else None,
            data=data,
            pagination=meta
        )
    )
