"""Pydantic models for schema definitions."""

from typing import Any
from pydantic import BaseModel, Field


class PropertySchema(BaseModel):
    """Schema definition for a Wikibase property."""
    
    id: str | None = Field(None, description="Property ID (P123)")
    label: str = Field(..., description="Property label")
    description: str = Field(..., description="Property description")
    datatype: str = Field(..., description="Property datatype")
    aliases: list[str] = Field(default_factory=list, description="Property aliases")
    constraints: dict[str, Any] = Field(default_factory=dict, description="Property constraints")


class ClaimSchema(BaseModel):
    """Schema definition for a Wikibase claim. Used for qualifiers and references."""
    id: str | None = Field(None, description="Claim property ID (P123)")
    label: str | None = Field(None, description="Claim property label")
    value: str = Field(..., description="Claim value")
    datatype: str = Field(..., description="Claim datatype")


class StatementSchema(BaseModel):
    """Schema definition for a Wikibase statement."""
    
    id: str | None = Field(None, description="Property ID (P123)")
    label: str | None = Field(None, description="Property label")
    value: str = Field(..., description="Value")
    datatype: str = Field(..., description="Datatype")
    qualifiers: list[ClaimSchema] | None = Field(default_factory=list, description="Qualifiers")
    references: list[ClaimSchema] | None = Field(default_factory=list, description="References")
    rank: str | None = Field(None, description="Rank")


class ItemSchema(BaseModel):
    """Schema definition for a Wikibase item."""
    
    id: str | None = Field(None, description="Item ID (Q123)")
    label: str = Field(..., description="Item label")
    description: str = Field(..., description="Item description")
    aliases: list[str] = Field(default_factory=list, description="Item aliases")
    statements: list[StatementSchema] = Field(default_factory=list, description="Default statements")


class SchemaConfig(BaseModel):
    """Schema configuration for syncing to Wikibase."""
    
    namespace: str = Field("", description="Namespace prefix for labels")
    language: str = Field("en", description="Default language code")
    properties: list[PropertySchema] = Field(default_factory=list, description="Properties to sync")
    items: list[ItemSchema] = Field(default_factory=list, description="Items to sync")
