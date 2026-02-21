# src/features/ocr_updates/pipeline/context_naming.py

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe_str(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _safe_join(parts: List[str], sep: str) -> str:
    parts = [p for p in (p.strip() for p in parts) if p]
    return sep.join(parts)


def _fmt_numeric(v: Any) -> str:
    """
    Normaliza numericos como:
      213.0 -> 213
      '213.9' -> 213.9
    """
    if v is None:
        return ""
    s = _safe_str(v)
    if not s:
        return ""
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
        return s
    except Exception:
        return s


def get_context_chain(db, entity_id: str, max_hops: int = 10) -> List[Dict[str, Any]]:
    """
    Devuelve cadena ordenada RAÍZ -> HOJA.
    Asume 'belongs_to' edges van de HIJO (_from) -> PADRE (_to).
    Usamos OUTBOUND para subir desde la entidad hasta la raíz.
    """
    # Verificamos si la colección de edges existe antes de consultar
    if not db.has_collection("belongs_to"):
        # Fallback si no hay grafo aun
        if db.has_collection("entities"):
            doc = db.collection("entities").get(entity_id)
            return [doc] if doc else []
        return []

    aql = """
    FOR v, e, p IN 0..@max_hops OUTBOUND DOCUMENT(CONCAT('entities/', @entity_id)) belongs_to
      // OPTIONS { uniqueVertices: "path" } // A veces causa problemas si hay ciclos, bfs es mas seguro para jerarquias
      RETURN v
    """
    # El AQL arriba devuelve los VÉRTICES individuales en orden de travesía:
    # 1. Entidad Inicial (Hijo)
    # 2. Padre
    # 3. Abuelo
    # ...

    cursor = db.aql.execute(aql, bind_vars={"entity_id": entity_id, "max_hops": max_hops})
    vertices = list(cursor)

    if not vertices:
        return []

    # Como Arango devuelve [Hijo, Padre, Abuelo], invertimos para tener [Abuelo, Padre, Hijo] (Raíz -> Hoja)
    return list(reversed(vertices))


def build_context_names(
    db,
    entity_id: Optional[str],
    required_document: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Construye:
      - name_path
      - code_path
      - code_numeric_path
      - name_code (padre-hoja)
      - name_code_numeric (padre-hoja)
      - display_name = name_code + timestamp
    Devuelve dict (robusto para .get()).
    """
    ts = _now_tag()

    required_document = required_document or {}

    if not entity_id:
        return {
            "display_name": f"document_{ts}",
            "name_code": "",
            "name_code_numeric": "",
            "name_path": "",
            "code_path": "",
            "code_numeric_path": "",
            "timestamp_tag": ts,
            "required_document_code": _safe_str(required_document.get("code")) or None,
        }

    chain = get_context_chain(db, entity_id)

    if not chain:
        return {
            "display_name": f"document_{ts}",
            "name_code": "",
            "name_code_numeric": "",
            "name_path": "",
            "code_path": "",
            "code_numeric_path": "",
            "timestamp_tag": ts,
            "required_document_code": _safe_str(required_document.get("code")) or None,
        }

    # Normaliza nodos
    norm = []
    for v in chain:
        norm.append({
            "_key": v.get("_key"),
            "name": v.get("name") or v.get("label") or "",
            "type": v.get("type") or "",
            "code": v.get("code"),
            "code_numeric": v.get("code_numeric"),
        })

    required_name = _safe_str(required_document.get("name"))
    required_code = _safe_str(required_document.get("code"))
    has_required_document_data = bool(required_name or required_code)

    if has_required_document_data:
        norm.append({
            "_key": required_document.get("id"),
            "name": required_name,
            "type": "required_document",
            "code": required_code,
            "code_numeric": None,
        })

    # paths completos
    name_path = _safe_join([_safe_str(n["name"]) for n in norm], " / ")
    code_path = _safe_join([_safe_str(n["code"]) for n in norm if _safe_str(n.get("code"))], " / ")
    code_numeric_path = _safe_join(
        [_fmt_numeric(n.get("code_numeric")) for n in norm if _fmt_numeric(n.get("code_numeric"))],
        " / "
    )

    # padre + hoja
    leaf = norm[-1]
    # Si la cadena tiene mas de 1 elemento, el penultimo es el padre inmediato
    parent = norm[-2] if len(norm) >= 2 else None

    target_name = _safe_str(leaf.get("name"))

    # Code combo
    if has_required_document_data:
        context_norm = norm[:-1]
        context_leaf = context_norm[-1] if context_norm else None
        context_parent = context_norm[-2] if len(context_norm) >= 2 else None

        if context_leaf:
            base_code_combo = _safe_join([
                _safe_str(context_parent.get("code")) if context_parent else "",
                _safe_str(context_leaf.get("code")),
            ], "-")
        else:
            base_code_combo = ""

        code_combo = _safe_join([base_code_combo, _safe_str(leaf.get("code"))], "-")
    else:
        if parent:
            code_combo = _safe_join([_safe_str(parent.get("code")), _safe_str(leaf.get("code"))], "-")
        else:
            code_combo = _safe_str(leaf.get("code"))

    name_code = f"{code_combo} - {target_name}".strip(" -") if code_combo else target_name

    # Numeric combo
    if has_required_document_data:
        context_norm = norm[:-1]
        context_leaf = context_norm[-1] if context_norm else None
        context_parent = context_norm[-2] if len(context_norm) >= 2 else None

        if context_leaf:
            num_combo = _safe_join([
                _fmt_numeric(context_parent.get("code_numeric")) if context_parent else "",
                _fmt_numeric(context_leaf.get("code_numeric")),
            ], "-")
        else:
            num_combo = ""
    else:
        if parent:
            num_combo = _safe_join([
                _fmt_numeric(parent.get("code_numeric")),
                _fmt_numeric(leaf.get("code_numeric")),
            ], "-")
        else:
            num_combo = _fmt_numeric(leaf.get("code_numeric"))

    name_code_numeric = f"{num_combo} - {target_name}".strip(" -") if num_combo else target_name

    display_name = f"{name_code} - {ts}".strip()

    return {
        "display_name": display_name,
        "name_code": name_code,
        "name_code_numeric": name_code_numeric,
        "name_path": name_path,
        "code_path": code_path,
        "code_numeric_path": code_numeric_path,
        "timestamp_tag": ts,
        "required_document_code": required_code or None,
        "path_nodes": norm,  # opcional por si quieres debug
    }
