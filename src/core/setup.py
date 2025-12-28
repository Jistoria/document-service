# src/core/setup.py
from arango.database import StandardDatabase


def init_arango_schema(db: StandardDatabase):
    """
    Crea las colecciones y aristas necesarias si no existen.
    """
    print("🛠️ Verificando esquema de ArangoDB...")

    # --- 1. Colecciones de Documentos (Nodos) ---
    doc_collections = [
        "entidades",  # Sedes, Facultades, Departamentos, Carreras
        "meta_schemas",  # Tus definiciones de formularios (JSON schemas)
        "dms_users"  # Para cuando conectes los usuarios
    ]

    for col in doc_collections:
        if not db.has_collection(col):
            db.create_collection(col)
            print(f"   ✅ Colección creada: {col}")

    # --- 2. Colecciones de Aristas (Relaciones) ---
    # ¡Importante! Estas deben crearse con edge=True
    edge_collections = [
        "pertenece_a",  # La jerarquía: Carrera -> Facultad -> Sede
        # Aquí añadirás otras en el futuro, ej: "firmado_por", "subido_por"
    ]

    for col in edge_collections:
        if not db.has_collection(col):
            db.create_collection(col, edge=True)
            print(f"   ✅ Colección de ARISTAS creada: {col}")

    print("✨ Esquema de base de datos verificado.")