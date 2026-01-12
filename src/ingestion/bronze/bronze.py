import logging
from datetime import datetime, timezone
from pyspark.sql import Row
from pathlib import Path

from extractors.core.models import document_metadata
from ingestion.delta import delta

DEFAULT_BINARY_FILE_NAME = "content.bin"
BRONZE_FILES_PATH = "data/bronze/files"
# On Fabric: /lakehouse/default/Files/bronze
BRONZE_DOCUMENTS_PATH = "data/bronze/bronze_documents"

logger = logging.getLogger(__name__)


def bronze_file_path(
    document_id: str,
    destination: str = BRONZE_FILES_PATH,
    binary_file_name: str = DEFAULT_BINARY_FILE_NAME,
    create_if_not_exists: bool = False,
) -> Path:
    path = Path(destination) / document_id
    if create_if_not_exists:
        logger.info(f"Creating path {path}")
        path.mkdir(parents=True, exist_ok=True)
    return path / binary_file_name


def read_file_bytes(file_id: str) -> bytes:
    path = bronze_file_path(file_id)
    if not path.exists():
        raise FileNotFoundError(f"File {path} not found")
    return path.read_bytes()


def write(
    document_id: str,
    file_name: str,
    file_extension: str,
    source_system: str = "local",
    *,
    path: str = BRONZE_DOCUMENTS_PATH,
):
    metadata = document_metadata.DocumentMetadata(
        document_id=document_id,
        file_name=file_name,
        file_extension=file_extension,
        source_system=source_system,
        ingested_at=datetime.now(timezone.utc),
    )
    logging.info(f"Writing metadata {metadata}")

    delta.write([metadata.model_dump()], path)


def read(
    path: str = BRONZE_DOCUMENTS_PATH,
) -> list[Row]:
    return delta.read(path).collect()
