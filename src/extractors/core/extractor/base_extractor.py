from abc import ABC, abstractmethod
from typing import Iterable

from extractors.core.models.document_unit import DocumentUnit


class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, document_id: str, file_bytes: bytes) -> Iterable[DocumentUnit]:
        pass
