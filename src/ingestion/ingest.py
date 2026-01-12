from pathlib import Path
import click
import shutil
import uuid

from extractors import loggingconfig
from ingestion.bronze import bronze

logger = loggingconfig.get_logger(__name__)


@click.command("ingest")
@click.argument("source_file", type=click.Path(exists=True))
def ingest_file(source_file: str):
    logger.info(f"Ingesting {source_file}")
    document_id = str(uuid.uuid4())
    source_path = Path(source_file)
    bronze_path = bronze.bronze_file_path(document_id, create_if_not_exists=True)

    logger.info(f"Copying file to {bronze_path}")
    shutil.copyfile(source_path, bronze_path)

    # Write metadata
    logger.info(f"Writing metadata {source_path.name=} {source_path.suffix=}")
    bronze.write(document_id, source_path.name, source_path.suffix)
