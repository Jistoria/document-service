from typing import Any, Dict, Optional

from src.core.database import db_instance


class ValidationRepository:
    def __init__(self):
        self.db = db_instance.get_db()

    def get_document_snapshot(self, doc_id: str) -> Optional[Dict[str, Any]]:
        aql = """
        FOR d IN documents
            FILTER d._key == @doc_id
            RETURN {
                _key: d._key,
                owner_id: d.owner.id,
                display_name: d.display_name,
                snap_context_name: d.snap_context_name
            }
        """
        result = list(self.db.aql.execute(aql, bind_vars={"doc_id": doc_id}))
        return result[0] if result else None

    def confirm_document(
        self,
        *,
        doc_id: str,
        clean_metadata: Dict[str, Any],
        is_public: bool,
        display_name: Optional[str],
        confirmed_by: str,
    ) -> Optional[Dict[str, Any]]:
        update_doc_aql = """
        FOR d IN documents
            FILTER d._key == @key

            LET has_new_display_name = @display_name != null
            LET display_name_changed = has_new_display_name AND @display_name != d.display_name

            LET original_snap_context_name = (
                HAS(d, 'snap_context_name') AND d.snap_context_name != null
            ) ? d.snap_context_name : d.display_name

            LET next_snap_context_name = display_name_changed
                ? original_snap_context_name
                : d.snap_context_name

            LET next_display_name = display_name_changed
                ? @display_name
                : d.display_name

            UPDATE d WITH {
                validated_metadata: @clean_data,
                status: 'confirmed',
                integrity_warnings: [],
                manually_validated_at: DATE_NOW(),
                confirmed_at: DATE_NOW(),
                confirmed_by: @confirmed_by,
                is_public: @is_public,
                is_locked: true,
                display_name: next_display_name,
                snap_context_name: next_snap_context_name
            } IN documents
            OPTIONS { mergeObjects: false }
            RETURN NEW
        """

        result = list(
            self.db.aql.execute(
                update_doc_aql,
                bind_vars={
                    "key": doc_id,
                    "clean_data": clean_metadata,
                    "is_public": is_public,
                    "display_name": display_name,
                    "confirmed_by": confirmed_by,
                },
            )
        )

        return result[0] if result else None
