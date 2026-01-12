from datetime import datetime
from pydantic import BaseModel


class DocumentMetadata(BaseModel):
    document_id: str
    file_name: str
    file_extension: str
    source_system: str
    ingested_at: datetime
