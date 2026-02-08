"""
Constructor de respuestas estandarizadas para el módulo de búsqueda.
"""
import math
from typing import List, Optional, Any, Dict
from .models import DocumentDetail, DocumentListResponse, DetailPagination, EntityRef


class ResponseBuilder:
    """Clase para construir respuestas estandarizadas."""
    
    @staticmethod
    def success_response(data: Any, message: str) -> Dict[str, Any]:
        """
        Construye una respuesta exitosa.
        
        Args:
            data: Datos a retornar
            message: Mensaje descriptivo
            
        Returns:
            dict: Respuesta estandarizada exitosa
        """
        return {
            "success": True,
            "data": data,
            "message": message
        }
    
    @staticmethod
    def error_response(message: str, data: Any = None) -> Dict[str, Any]:
        """
        Construye una respuesta de error.
        
        Args:
            message: Mensaje de error
            data: Datos opcionales
            
        Returns:
            dict: Respuesta estandarizada de error
        """
        return {
            "success": False,
            "data": data,
            "message": message
        }
    
    @staticmethod
    def build_document_detail_response(doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construye respuesta para un documento individual.
        
        Args:
            doc_data: Datos del documento desde ArangoDB
            
        Returns:
            dict: Respuesta con DocumentDetail
        """
        return ResponseBuilder.success_response(
            data=DocumentDetail(**doc_data),
            message="Documento encontrado exitosamente."
        )
    
    @staticmethod
    def build_empty_list_response(
        page: int, 
        page_size: int, 
        message: str = "No se encontraron documentos."
    ) -> Dict[str, Any]:
        """
        Construye respuesta para lista vacía.
        
        Args:
            page: Número de página actual
            page_size: Tamaño de página
            message: Mensaje descriptivo
            
        Returns:
            dict: Respuesta con lista vacía y paginación
        """
        return ResponseBuilder.success_response(
            data=DocumentListResponse(
                data=[],
                pagination=DetailPagination(
                    currentPage=page,
                    lastPage=1,
                    perPage=page_size,
                    total=0,
                    to=0,
                    hasMorePages=False
                )
            ),
            message=message
        )
    
    @staticmethod
    def build_paginated_response(
        items_data: List[Dict[str, Any]],
        total_items: int,
        page: int,
        page_size: int
    ) -> Dict[str, Any]:
        """
        Construye respuesta paginada con documentos.
        
        Args:
            items_data: Lista de datos de documentos
            total_items: Total de items encontrados
            page: Página actual
            page_size: Tamaño de página
            
        Returns:
            dict: Respuesta con DocumentListResponse y paginación
        """
        # Convertir datos a modelos
        items_list = [DocumentDetail(**d) for d in items_data]
        
        # Calcular paginación
        offset = (page - 1) * page_size
        last_page = max(1, math.ceil(total_items / page_size))
        to_item = offset + len(items_list)
        has_more = page < last_page
        
        # Construir respuesta
        internal_data = DocumentListResponse(
            data=items_list,
            pagination=DetailPagination(
                currentPage=page,
                lastPage=last_page,
                perPage=page_size,
                total=total_items,
                to=to_item,
                hasMorePages=has_more
            )
        )
        
        return ResponseBuilder.success_response(
            data=internal_data,
            message="Búsqueda completada exitosamente."
        )
    
    @staticmethod
    def build_entities_response(entities_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Construye respuesta para lista de entidades.
        
        Args:
            entities_data: Lista de datos de entidades
            
        Returns:
            dict: Respuesta con lista de EntityRef
        """
        entities = [EntityRef(**d) for d in entities_data]
        return ResponseBuilder.success_response(
            data=entities,
            message=f"Se encontraron {len(entities)} entidades con documentos."
        )
