"""Pydantic models for schema definitions."""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class PropertySchema(BaseModel):
    """Schema definition for a Wikibase property."""
    
    id: Optional[str] = Field(None, description="Property ID (P123)")
    label: str = Field(..., description="Property label")
    description: str = Field(..., description="Property description")
    datatype: str = Field(..., description="Property datatype")
    aliases: List[str] = Field(default_factory=list, description="Property aliases")
    constraints: Dict[str, Any] = Field(default_factory=dict, description="Property constraints")


class ItemSchema(BaseModel):
    """Schema definition for a Wikibase item."""
    
    id: Optional[str] = Field(None, description="Item ID (Q123)")
    label: str = Field(..., description="Item label")
    description: str = Field(..., description="Item description")
    aliases: List[str] = Field(default_factory=list, description="Item aliases")
    statements: Dict[str, Any] = Field(default_factory=dict, description="Default statements")


class SchemaConfig(BaseModel):
    """Schema configuration for syncing to Wikibase."""
    
    namespace: str = Field("", description="Namespace prefix for labels")
    language: str = Field("en", description="Default language code")
    properties: List[PropertySchema] = Field(default_factory=list, description="Properties to sync")
    items: List[ItemSchema] = Field(default_factory=list, description="Items to sync")
