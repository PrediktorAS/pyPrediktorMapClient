from typing import Dict, List, Any
from abc import ABC, abstractmethod


class IDWH(ABC):
    @abstractmethod
    def version(self) -> Dict:
        pass

    @abstractmethod
    def fetch(self, query: str, to_dataframe: bool = False) -> List[Any]:
        pass

    @abstractmethod
    def execute(self, query: str, commit: bool = True) -> List[Any]:
        pass
