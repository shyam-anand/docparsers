from extractors.core.extractor.base_extractor import BaseExtractor
from extractors.core.models.document_unit import DocumentUnit
from typing import Any, List

import pdfplumber
from io import BytesIO


class PdfExtractor(BaseExtractor):
    parser_name = "pdfplumber"
    parser_version = "1.0"

    def extract(self, document_id: str, file_bytes: bytes) -> List[DocumentUnit]:
        units = []

        with pdfplumber.open(BytesIO(file_bytes)) as pdf:
            for idx, page in enumerate[Any](pdf.pages):
                text = page.extract_text() or ""

                tables = []
                for table in page.extract_tables():
                    tables.append(
                        {
                            "rows": table,
                            "columns": table[0] if table else [],
                        }
                    )

                layout = {
                    "page_number": idx + 1,
                    "width": page.width,
                    "height": page.height,
                }

                units.append(
                    DocumentUnit(
                        document_id=document_id,
                        unit_type="page",
                        unit_index=idx,
                        unit_name=None,
                        raw_text=text,
                        tables=tables,
                        layout=layout,
                        parser_name=self.parser_name,
                        parser_version=self.parser_version,
                    )
                )

        return units
