from typing import List, Dict, Any


class DocumentUnit:
    def __init__(
        self,
        document_id: str,
        unit_type: str,
        unit_index: int,
        unit_name: str | None,
        raw_text: str,
        tables: List[Dict[str, Any]],
        layout: Dict[str, Any],
        parser_name: str,
        parser_version: str,
    ):
        self.document_id = document_id
        self.unit_type = unit_type
        self.unit_index = unit_index
        self.unit_name = unit_name
        self.raw_text = raw_text
        self.tables = tables
        self.layout = layout
        self.parser_name = parser_name
        self.parser_version = parser_version

    def to_dict(self) -> Dict[str, Any]:
        return {
            "document_id": self.document_id,
            "unit_id": None,  # filled later
            "unit_type": self.unit_type,
            "unit_index": self.unit_index,
            "unit_name": self.unit_name,
            "raw_text": self.raw_text,
            "tables_json": self.tables,
            "layout_json": self.layout,
            "parser_name": self.parser_name,
            "parser_version": self.parser_version,
        }
