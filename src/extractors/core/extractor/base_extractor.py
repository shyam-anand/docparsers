from abc import ABC, abstractmethod
from typing import List

from extractors.core.models.document_unit import DocumentUnit


class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, document_id: str, file_bytes: bytes) -> List[DocumentUnit]:
        pass
