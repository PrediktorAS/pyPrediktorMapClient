from abc import ABC, abstractmethod
from typing import Dict, List


class IDWH(ABC):
    @abstractmethod
    def version(self) -> Dict:
        raise NotImplementedError("version method is not implemented")

    @abstractmethod
    def fetch(self, query: str, to_dataframe: bool = False) -> List:
        raise NotImplementedError("fetch method is not implemented")

    @abstractmethod
    def execute(self, query: str, *args, **kwargs) -> List:
        raise NotImplementedError("execute method is not implemented")
