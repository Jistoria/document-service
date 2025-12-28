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
    Devuelve cadena ordenada RAÍZ -> HOJA usando p.vertices para orden estable.
    Asume:
      - vertices: entidades
      - edge collection: pertenece_a (child -> parent)
    """
    aql = """
    FOR v, e, p IN 0..@max_hops OUTBOUND DOCUMENT(CONCAT('entidades/', @entity_id)) pertenece_a
      OPTIONS { uniqueVertices: "path" }
      LIMIT 1
      RETURN p.vertices
    """
    cursor = db.aql.execute(aql, bind_vars={"entity_id": entity_id, "max_hops": max_hops})
    rows = list(cursor)

    if not rows or not rows[0]:
        # fallback: devolver solo la entidad actual
        if db.has_collection("entidades"):
            doc = db.collection("entidades").get(entity_id)
            return [doc] if doc else []
        return []

    vertices = rows[0]  # [start, parent, grandparent, ...]
    # Queremos raíz -> hoja
    return list(reversed(vertices))


def build_context_names(db, entity_id: Optional[str]) -> Dict[str, Any]:
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

    if not entity_id:
        return {
            "display_name": f"document_{ts}",
            "name_code": "",
            "name_code_numeric": "",
            "name_path": "",
            "code_path": "",
            "code_numeric_path": "",
            "timestamp_tag": ts,
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

    # paths completos
    name_path = _safe_join([_safe_str(n["name"]) for n in norm], " / ")
    code_path = _safe_join([_safe_str(n["code"]) for n in norm if _safe_str(n.get("code"))], " / ")
    code_numeric_path = _safe_join(
        [_fmt_numeric(n.get("code_numeric")) for n in norm if _fmt_numeric(n.get("code_numeric"))],
        " / "
    )

    # padre + hoja
    leaf = norm[-1]
    parent = norm[-2] if len(norm) >= 2 else None

    code_combo = _safe_join([_safe_str(parent.get("code")) if parent else "", _safe_str(leaf.get("code"))], "-")
    name_code = f"{code_combo} - {_safe_str(leaf.get('name'))}".strip(" -") if code_combo else _safe_str(leaf.get("name"))

    num_combo = _safe_join([
        _fmt_numeric(parent.get("code_numeric")) if parent else "",
        _fmt_numeric(leaf.get("code_numeric")),
    ], "-")
    name_code_numeric = f"{num_combo} - {_safe_str(leaf.get('name'))}".strip(" -") if num_combo else _safe_str(leaf.get("name"))

    display_name = f"{name_code} - {ts}".strip()

    return {
        "display_name": display_name,
        "name_code": name_code,
        "name_code_numeric": name_code_numeric,
        "name_path": name_path,
        "code_path": code_path,
        "code_numeric_path": code_numeric_path,
        "timestamp_tag": ts,
        "path_nodes": norm,  # opcional por si quieres debug
    }
