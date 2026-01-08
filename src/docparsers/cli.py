import click

from docparsers import loggingconfig
from docparsers.excelparser import main as excelparser
from docparsers.pdfparser import main as pdfparser

logger = loggingconfig.get_logger(__name__)


@click.group()
@click.option("-v", "--verbose", count=True, help="Increase verbosity")
def cli(verbose: int = 0) -> None:
    loggingconfig.set_log_level(verbose)


cli.add_command(pdfparser.parse_pdf)
cli.add_command(excelparser.parse_excel)

if __name__ == "__main__":
    cli()
