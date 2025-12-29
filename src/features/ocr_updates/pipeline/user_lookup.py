import logging
from typing import Any, Dict, Optional, List
from difflib import SequenceMatcher  # 👈 IMPORTANTE: Para comparar similitud de texto

from src.core.config import settings
from .person_normalizer import build_search_terms
from .graph_client import MicrosoftGraphClient
from .users_repository import upsert_user_from_graph

logger = logging.getLogger(__name__)


def calculate_similarity(a: str, b: str) -> float:
    """Calcula un ratio de similitud entre 0.0 y 1.0."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


async def lookup_user_in_microsoft_graph(db, raw_text: str) -> Optional[Dict[str, Any]]:
    """
    Busca en Microsoft Graph y aplica un filtro estricto de similitud.
    """
    # 1. Preparar términos
    name, email, parts = build_search_terms(raw_text)

    # 2. Validar credenciales
    tenant = getattr(settings, "AZURE_TENANT_ID", None)
    client_id = getattr(settings, "AZURE_CLIENT_ID", None)
    client_secret = getattr(settings, "AZURE_CLIENT_SECRET", None)

    if not (tenant and client_id and client_secret):
        logger.warning("⚠️ Faltan credenciales Azure para Graph.")
        return None

    try:
        # 3. Consultar API (Traemos más candidatos para poder filtrar después)
        graph = MicrosoftGraphClient(tenant_id=tenant, client_id=client_id, client_secret=client_secret)
        # Aumentamos el limit para tener de dónde escoger si el primero es malo
        limit = 15

        logger.info(f"☁️ Consultando Graph para: '{raw_text}' (Limit: {limit})")

        # Si el nombre es muy corto, Graph puede fallar o traer demasiados.
        if len(raw_text) < 4:
            return None

        candidates = await graph.search_users_optimized(parts=parts, limit=limit)

        if not candidates:
            logger.info("☁️ Graph no devolvió resultados.")
            return None

        # --- 4. FILTRO ESTRICTO Y RE-RANKING (LA MEJORA CLAVE) ---
        # No confiamos ciegamente en el primer resultado de Graph.
        # Comparamos el texto del OCR contra el displayName de cada candidato.

        best_candidate = None
        best_score = 0.0

        # Umbral de aceptación (0.75 = 75% de similitud).
        # Ajusta a 0.8 si quieres ser aun más estricto.
        SIMILARITY_THRESHOLD = 0.75

        # Si tenemos email, es el "Golden Ticket", gana automáticamente
        if email:
            em = email.lower()
            for u in candidates:
                m = (u.get("mail") or "").lower()
                upn = (u.get("userPrincipalName") or "").lower()
                if em in (m, upn):
                    best_candidate = u
                    best_score = 1.0
                    logger.info(f"🎯 Match exacto por Email: {email}")
                    break

        # Si no hubo match de email, usamos fuerza bruta de similitud de nombres
        if not best_candidate:
            logger.info(f"🔍 Evaluando {len(candidates)} candidatos de Graph...")

            for cand in candidates:
                # Construimos el nombre completo del candidato para comparar
                cand_display = cand.get("displayName", "")
                cand_full = f"{cand.get('givenName', '')} {cand.get('surname', '')}"

                # Probamos similitud contra el DisplayName y contra Name+Surname
                score_display = calculate_similarity(raw_text, cand_display)
                score_full = calculate_similarity(raw_text, cand_full)

                # Nos quedamos con el mejor score de este candidato
                score = max(score_display, score_full)

                # Log de debug para ver por qué acepta o rechaza
                # logger.debug(f"   vs '{cand_display}': {score:.2f}")

                if score > best_score:
                    best_score = score
                    best_candidate = cand

        # 5. Decisión Final
        if best_candidate and best_score >= SIMILARITY_THRESHOLD:
            logger.info(f"✅ Match Graph Aceptado: '{best_candidate.get('displayName')}' (Score: {best_score:.2f})")

            graph_payload = {
                "azure_id": best_candidate.get("id"),
                "displayName": best_candidate.get("displayName"),
                "mail": best_candidate.get("mail"),
                "userPrincipalName": best_candidate.get("userPrincipalName"),
                "givenName": best_candidate.get("givenName"),
                "surname": best_candidate.get("surname"),
                "jobTitle": best_candidate.get("jobTitle"),
                "department": best_candidate.get("department"),
                "officeLocation": best_candidate.get("officeLocation"),
                "type": "usuario"
            }

            return upsert_user_from_graph(db, graph_user=graph_payload, source="graph_fallback")

        else:
            # Si el mejor score es muy bajo (ej: 0.4), es que encontramos a "Tito Mieles" buscando a "Diego Mieles"
            logger.warning(
                f"⛔ Match Graph Rechazado. Mejor candidato: '{best_candidate.get('displayName') if best_candidate else 'N/A'}' con Score {best_score:.2f} (Umbral: {SIMILARITY_THRESHOLD})")
            return None

    except Exception as e:
        logger.error(f"❌ Error consultando Microsoft Graph: {e}", exc_info=True)
        return None