from typing import Dict, List
from abc import ABC, abstractmethod


class IDWH(ABC):
    @abstractmethod
    def version(self) -> Dict:
        pass

    @abstractmethod
    def fetch(self, query: str, to_dataframe: bool = False) -> List:
        pass

    @abstractmethod
    def execute(self, query: str, *args, **kwargs) -> List:
        pass
