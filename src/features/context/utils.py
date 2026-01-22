from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

def resolve_team_codes(db, allowed_teams: List[str], return_full_object: bool = False) -> List[Dict[str, Any]]:
    """
    Traduce los códigos de permisos (ej: 'CARR:213.11', 'FAC:10')
    a entidades reales en ArangoDB.
    
    Args:
        db: Instancia de base de datos ArangoDB.
        allowed_teams: Lista de strings con formato 'TIPO:CODIGO'.
        return_full_object: Si es True, retorna objetos completos con formato para frontend.
                            Si es False, retorna solo los _key (IDs).
    """
    if not allowed_teams or "*" in allowed_teams:
        return []

    # 1. Estructura de mapeo (Prefijo Redis -> type en Arango)
    type_map = {
        "CARR": "carrera",
        "FAC": "facultad",
        "DEP": "departamento"
        # Agregar otros si son necesarios
    }

    # 2. Preparar filtros para AQL
    criteria = []
    for team in allowed_teams:
        if ":" in team:
            prefix, code = team.split(":", 1)
            # Limpiar espacios por si acaso
            code = code.strip()
            if prefix in type_map:
                criteria.append({
                    "type": type_map[prefix],
                    "code": code
                })

    if not criteria:
        return []

    logger.debug(f"Searching entities with criteria: {criteria}")

    # 3. Consulta AQL
    # Usamos un patrón de subconsulta (LET e = FIRST(...)) para garantizar 
    # que el LIMIT 1 aplique por cada criterio individualmente.
    
    return_stmt = "e._key"
    if return_full_object:
        # Formato esperado por el frontend en /me/entities
        # OJO: code_numeric puede ser null, usamos fallback a code
        return_stmt = """
        {
            id: e._key,
            name: e.name,
            code: e.code,
            type: CONCAT(UPPER(SUBSTRING(e.type, 0, 1)), LOWER(SUBSTRING(e.type, 1))),
            teamId: CONCAT(
                (e.type == 'carrera' ? 'CARR:' : 
                 e.type == 'facultad' ? 'FAC:' : 
                 e.type == 'departamento' ? 'DEP:' : 'UNK:'), 
                (e.code_numeric != null ? TO_STRING(e.code_numeric) : e.code)
            )
        }
        """

    # NOTA: Usamos 'doc' dentro del subquery para no confundir variables, 
    # y asignamos a 'e' afuera para que coincida con return_stmt.
    # CRITICO: Forzamos TO_STRING para comparar códigos numéricos (ej: 213 vs "213")
    aql = f"""
    FOR c IN @criteria
        LET e = FIRST(
            FOR doc IN entities
                FILTER doc.type == c.type 
                   AND (
                        doc.code == c.code 
                        OR TO_STRING(doc.code) == c.code
                        OR TO_STRING(doc.code_numeric) == c.code
                   )
                LIMIT 1
                RETURN doc
        )
        FILTER e != null
        RETURN {return_stmt}
    """

    cursor = db.aql.execute(aql, bind_vars={"criteria": criteria})
    results = list(cursor)
    
    logger.debug(f"Resolved teams: {allowed_teams} -> found {len(results)} matches")
    return results
