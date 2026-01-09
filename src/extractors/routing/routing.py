from extractors.core.extractor.base_extractor import BaseExtractor
from extractors.pdfextractor.pdf_extractor import PdfExtractor
from extractors.excelextractor.excel_extractor import ExcelExtractor


def get_extractor(file_extension: str) -> BaseExtractor:
    match file_extension.lower():
        case "pdf":
            return PdfExtractor()
        case "xlsx" | "xls":
            return ExcelExtractor()
        # case "pptx":
        #     return PPTExtractor()
        case _:
            raise ValueError(f"Unsupported file type: {file_extension}")
