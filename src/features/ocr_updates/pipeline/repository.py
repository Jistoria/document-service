from typing import Any, Dict

DOCUMENTS_COLLECTION = "documents"

def ensure_documents_collection(db):
    if not db.has_collection(DOCUMENTS_COLLECTION):
        db.create_collection(DOCUMENTS_COLLECTION)

def upsert_document(db, document_record: Dict[str, Any]):
    ensure_documents_collection(db)
    db.collection(DOCUMENTS_COLLECTION).insert(document_record, overwrite=True)
