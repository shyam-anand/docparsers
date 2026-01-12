import pandas as pd
from extractors.excel import excel_extractor
from ingestion.bronze import bronze
from ingestion.delta import delta

_SILVER_DOCUMENTS_PATH = "data/silver/silver_document_units"


def write(path: str = _SILVER_DOCUMENTS_PATH):
    silver_rows = []

    extractor = excel_extractor.ExcelExtractor()
    documents = bronze.read()
    for doc in documents:
        file_bytes = bronze.read_file_bytes(doc.document_id)
        silver_rows.extend(
            [
                document_unit.to_dict()
                for document_unit in extractor.extract(doc.document_id, file_bytes)
            ]
        )

    delta.write(pd.DataFrame(silver_rows), path)
