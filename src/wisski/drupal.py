from typing import Optional
from dataclasses import dataclass


@dataclass
class Field:
    id: str
    field_type: str
    display_widget: Optional[str] = ""
    formatter_widget: Optional[str] = ""
    cardinality: int = -1
    field_type_informative: Optional[str] = ""
    weight: int = 0


@dataclass
class Bundle: ...
