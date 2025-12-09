from __future__ import annotations
from typing import List, Union, Any, Dict, Literal
from pydantic import BaseModel, Field, model_validator
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


class SnakMatcher(BaseModel):
    """Matches items by a property-value pair (snak)."""
    property: str = Field(
        ..., description="Property label for matching"
    )
    value: str = Field(
        ..., description="Value template for matching (e.g. '{column}')"
    )


class ItemSearchMode(str, Enum):
    """Determines how to search for existing items."""
    LABEL = "label"
    LABEL_DESCRIPTION = "label_description"
    LABEL_SNAK = "label_snak"


class ItemDefinition(BaseModel):
    """Defines how to identify/search for an item in Wikibase.

    Search modes (determined automatically):
    - label only: Search by label, raises error if ambiguous (multiple matches)
    - label + description: Search by label and description (unique combination)
    - label + snak: Search by label and property-value pair (snak)
    """
    label: str = Field(..., description="Label template (required)")
    description: str | None = Field(
        None, description="Description template for label+description search"
    )
    snak: SnakMatcher | None = Field(
        None, description="Property-value matcher for label+snak search"
    )

    @property
    def search_mode(self) -> ItemSearchMode:
        """Determine the search mode based on provided fields."""
        if self.snak:
            return ItemSearchMode.LABEL_SNAK
        if self.description:
            return ItemSearchMode.LABEL_DESCRIPTION
        return ItemSearchMode.LABEL

class ValueDefinition(BaseModel):
    label: str | None = None
    snak: SnakMatcher | None = None
    
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
    chunk_size: int | None = None
    csv_files: List[CSVFileConfig]
