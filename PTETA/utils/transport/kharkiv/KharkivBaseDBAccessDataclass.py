from abc import ABC
from dataclasses import dataclass
from transport.BaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass
class BaseDBAccessDataclass(BaseDBAccessDataclass, ABC):
    @classmethod
    def __schema_name__(cls) -> str:
        return "kharkiv"
