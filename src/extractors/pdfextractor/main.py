import click
from extractors.pdfextractor import pdfparser
from pathlib import Path


@click.command("pdf")
@click.argument("file", type=click.Path(exists=True))
def parse_pdf(file: str):
    pdf_parser = pdfparser.PdfParser()
    for page_data in pdf_parser.parse(Path(file)):
        print(f"Page {page_data.page_number}")
        print(f"{len(page_data.tables)} tables")
        for table in page_data.tables:
            print("#" * 100)
            for row in table:
                print("-" * 100)
                for cell in row:
                    if cell:
                        print(cell, end=" ")
                    else:
                        print("<blank>", end=" ")
                print()
            print("#" * 100)

        print("*** TEXT ***")
        print(page_data.text)
        print("*** END TEXT ***")
        input("\n\nPress Enter to continue...")


if __name__ == "__main__":
    parse_pdf()
