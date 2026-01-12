import click

from extractors import loggingconfig
from extractors.excel import main as excelparser
from extractors.pdf import main as pdfparser
from ingestion import ingest
from ingestion.delta import cli as delta_cli

logger = loggingconfig.get_logger(__name__)


@click.group()
@click.option("-v", "--verbose", count=True, help="Increase verbosity")
def cli(verbose: int = 0) -> None:
    loggingconfig.set_log_level(verbose)


cli.add_command(pdfparser.parse_pdf)
cli.add_command(excelparser.parse_excel)
cli.add_command(ingest.ingest_file)
cli.add_command(delta_cli.read_delta)

if __name__ == "__main__":
    cli()
