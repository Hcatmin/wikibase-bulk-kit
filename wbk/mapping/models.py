"""Pydantic models for CSV to Wikibase mapping configurations."""

from pydantic import BaseModel, Field


class ClaimMapping(BaseModel):
    property_id: str = Field(..., description="Property ID")
    property_label: str = Field(..., description="Property label")
    value_column: str = Field(..., description="Column that contains the value")
    datatype: str = Field(..., description="Datatype")


class StatementMapping(BaseModel):
    property_id: str = Field(..., description="Property ID")
    property_label: str = Field(..., description="Property label")
    value_column: str = Field(..., description="Column that contains the value")
    datatype: str = Field(..., description="Datatype")
    qualifiers: list[ClaimMapping] = Field(default_factory=list, description="Qualifiers")
    references: list[ClaimMapping] = Field(default_factory=list, description="References")
    rank: str = Field(..., description="Rank")

class ItemMapping(BaseModel):
    label_column: str = Field(..., description="The column that contains the item label")
    description: str | None = Field(None, description="Static description or built from other columns")
    aliases_columns: list[str] = Field(default_factory=list, description="Columns that contain item aliases")
    statements: list[StatementMapping] | None = Field(None, description="Default statements")

class CSVFileConfig(BaseModel):
    file_path: str = Field(..., description="Path to the CSV file")
    encoding: str | None = Field(None, description="Specific file encoding")
    delimiter: str | None = Field(None, description="Specific CSV delimiter")
    decimal_separator: str | None = Field(None, description="Specific decimal separator")

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