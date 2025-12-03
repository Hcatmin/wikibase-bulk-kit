from __future__ import annotations
from typing import List, Union, Any, Dict
from pydantic import BaseModel, Field
from enum import Enum

# Flexible value specification used throughout the mapping pipeline. Supports
# raw literals, template strings, dictionaries, nested value definitions or
# lists of any of these.
ValueSpec = Union[str, List[Any], Dict[str, Any], "ValueDefinition", Any]

class UpdateAction(str, Enum):
    APPEND_OR_REPLACE = "append_or_replace"
    FORCE_APPEND = "force_append"
    KEEP = "keep"
    REPLACE_ALL = "replace_all"
    MERGE_REFS_OR_APPEND = "merge_refs_or_append"
    MERGE_QUALIFIERS_OR_APPEND = "merge_qualifiers_or_append"

class UniqueKey(BaseModel):
    property: str = Field(..., description="Property label for the unique key")
    value: str = Field(..., description="Value template for the unique key (e.g. '{column}')")

class ItemDefinition(BaseModel):
    label: str = Field(..., description="Label template")
    unique_key: UniqueKey | None = None
    description: str | None = None

class ValueDefinition(BaseModel):
    label: str | None = None
    unique_key: UniqueKey | None = None
    
class StatementDefinition(BaseModel):
    property: str = Field(..., description="Property label")
    value: Union[str, List[Any], ValueDefinition, Any] = Field(..., description="Value specification")
    qualifiers: list[StatementDefinition] | None = None
    references: list[StatementDefinition] | None = None
    rank: str | None = None
    
class MappingRule(BaseModel):
    update_action: UpdateAction | None = None
    item: ItemDefinition
    statements: List[StatementDefinition] | None = None

class CSVFileConfig(BaseModel):
    file_path: str
    encoding: str | None = None
    delimiter: str | None = None
    decimal_separator: str | None = None

    update_action: UpdateAction | None = None
    mappings: List[MappingRule]

class MappingConfig(BaseModel):
    name: str
    language: str = "es"
    encoding: str = "utf-8"
    delimiter: str = ","
    decimal_separator: str = "."
    csv_files: List[CSVFileConfig]
