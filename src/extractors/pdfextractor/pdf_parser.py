import logging
import pathlib
import re
from typing import Iterable

import pdfplumber
from pdfplumber.pdf import PDF
from pdfplumber.page import Page
from pdfplumber.table import Table
from pydantic import BaseModel

from extractors import config

logger = logging.getLogger(__name__)


NON_LATIN_RE = re.compile(r"[^\x00-\x7f]")


class PageData(BaseModel):
    page_number: int
    text: str
    tables: list[list[list[str | None]]]
    method: str


class PdfParser:
    def extract_table_rows(self, table: Table) -> Iterable[list[str]]:
        table_cells = table.cells
        logger.debug(f"Table cells: {table_cells}")
        input()

        for row in table.rows:
            logger.debug(f"Row: {row.cells}")
            input()

        table_rows = table.extract()
        logger.debug(f"Table rows: {table_rows}")

        row_count = 0
        for row in table_rows:
            row_count += 1
            cells = [cell for cell in row if cell and cell.strip()]
            if cells:
                yield cells

    def extract_text_from_pdf(self, pdf_page: Page, remove_non_latin: bool = True) -> Iterable[str]:
        try:
            extracted_text = pdf_page.extract_text()
            text = NON_LATIN_RE.sub("", extracted_text) if remove_non_latin else extracted_text

            yield text

        except Exception as e:
            logger.error(f"Error extracting text from {pdf_page}: {e}")

    def _open_pdf_file(self, file_path: pathlib.Path) -> PDF:
        file_path = config.APP_ROOT / file_path if not file_path.is_absolute() else file_path
        if not file_path.exists():
            raise FileNotFoundError(f"File {file_path} not found")
        if not file_path.suffix == ".pdf":
            raise ValueError(f"File {file_path} is not a PDF file")

        logger.info(f"Opening {file_path}")
        return pdfplumber.open(file_path)

    def parse(
        self,
        file_path: pathlib.Path,
    ) -> Iterable[PageData]:
        pdf = self._open_pdf_file(file_path)
        logger.info(f"Extracting from {file_path} ")

        for page_number, page in enumerate[Page](pdf.pages):
            logger.debug(f"On page {page.page_number} ({page_number})")
            if tables := page.find_tables():
                for table_number, table in enumerate[Table](tables, start=1):
                    logger.debug(f"On table {table_number} ({table.bbox})")
                    for row in self.extract_table_rows(table):
                        if row:
                            logger.info("\t".join([f"[{cell}]" for cell in row]))
                            input("Press Enter to continue...")

            else:
                print("No tables in page {page.page_number} ({page_number})")
                input("Press Enter to continue...")

            # images = page.images
            # logger.info(f"Ignoring {len(images)} images")

            # if tables := page.find_tables():
            #     for row in extract_tables_from_page(tables):
            #         if row:
            #             logger.info("\t".join([f"[{cell}]" for cell in row]))

            yield PageData(
                page_number=page_number,
                text=page.extract_text(),
                tables=page.extract_tables(),
                method="pdfplumber",
            )

        # was_extracted = False
        # for page_data in self.extract_text_from_pdf(pdf):
        #     if not page_data.text:
        #         continue

        #     logger.debug(f"Processing text: {page_data.text[:500]}")

        #     was_extracted = True
        #     yield page_data

        # if not was_extracted:
        #     logger.warning(f"No text extracted from {file_path}")
        #     # TODO Use OCR to extract text
