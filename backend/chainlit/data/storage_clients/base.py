from abc import ABC, abstractmethod
from typing import Any, Dict, Union

EXPIRY_TIME = 60 * 60 * 24 * 7  # 7 days


class BaseStorageClient(ABC):
    """Base class for non-text data persistence like Azure Data Lake, S3, Google Storage, etc."""

    @abstractmethod
    async def upload_file(
        self,
        object_key: str,
        data: Union[bytes, str],
        mime: str = "application/octet-stream",
        overwrite: bool = True,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def delete_file(self, object_key: str) -> bool:
        pass

    @abstractmethod
    async def get_read_url(self, object_key: str) -> str:
        pass
