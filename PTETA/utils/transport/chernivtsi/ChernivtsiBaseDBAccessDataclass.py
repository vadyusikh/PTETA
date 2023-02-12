from abc import ABC
from dataclasses import dataclass
from PTETA.utils.transport.BaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass
class ChernivtsiBaseDBAccessDataclass(BaseDBAccessDataclass, ABC):
    @classmethod
    def __schema_name__(cls) -> str:
        return "pteta_v2"
