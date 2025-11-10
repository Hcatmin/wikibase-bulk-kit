"""Pydantic models for CSV to Wikibase mapping configurations."""

from pydantic import BaseModel, Field
from enum import Enum
from typing import Union

class UpdateAction(str, Enum):
    APPEND_OR_REPLACE = "append_or_replace"
    FORCE_APPEND = "force_append"
    KEEP = "keep"
    REPLACE_ALL = "replace_all"
    MERGE_REFS_OR_APPEND = "merge_refs_or_append"

# Value specification types
ValueSpec = Union[
    str,  # Shorthand: column name
    dict,  # Explicit: {"column": "col_name"} or {"value": "static"} or {"label": "label_name"}
    list  # Tuple: [{"column": "col1"}, {"value": "static"}, ...]
]

class ClaimMapping(BaseModel):
    property_id: str | None = Field(None, description="Property ID")
    property_label: str | None = Field(None, description="Property label")
    value: ValueSpec | None = Field(None, description="Value specification: column name (str), explicit dict, or tuple list")
    datatype: str = Field(..., description="Datatype")


class StatementMapping(BaseModel):
    property_id: str | None = Field(None, description="Property ID")
    property_label: str | None = Field(None, description="Property label")
    value: ValueSpec | None = Field(None, description="Value specification: column name (str), explicit dict, or tuple list")
    datatype: str = Field(..., description="Datatype")
    qualifiers: list[ClaimMapping] | None = Field(default_factory=list, description="Qualifiers")
    references: list[ClaimMapping] | None = Field(default_factory=list, description="References")
    rank: str | None = Field(None, description="Rank")


class ItemMapping(BaseModel):
    update_action: UpdateAction | None = Field(None, description="Action to take when updating the item")

    label_column: str = Field(..., description="The column that contains the item label")
    description: str | None = Field(None, description="Static description or built from other columns")
    aliases_columns: list[str] = Field(default_factory=list, description="Columns that contain item aliases")
    statements: list[StatementMapping] | None = Field(None, description="Default statements")


class CSVFileConfig(BaseModel):
    file_path: str = Field(..., description="Path to the CSV file")
    encoding: str | None = Field(None, description="Specific file encoding")
    delimiter: str | None = Field(None, description="Specific CSV delimiter")
    decimal_separator: str | None = Field(None, description="Specific decimal separator")

    update_action: UpdateAction | None = Field(None, description="Action to take when updating the item")

    item_mapping: list[ItemMapping] | None = Field(None, description="Item mapping")


class MappingConfig(BaseModel):
    """Main mapping configuration for CSV to Wikibase transformation."""
    name: str = Field(..., description="Mapping configuration name")
    description: str | None = Field(None, description="Mapping description")
    language: str = Field("en", description="Language code")

    encoding: str = Field("utf-8", description="Default file encoding")
    delimiter: str = Field(",", description="Default CSV delimiter")
    decimal_separator: str = Field(".", description="Default decimal separator")
    
    csv_files: list[CSVFileConfig] = Field(..., description="CSV files to process")