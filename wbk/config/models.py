"""Pydantic models for configuration validation."""

from typing import Any
from pydantic import BaseModel, Field


class WikibaseConfig(BaseModel):
    """Wikibase instance configuration."""
    
    url: str = Field(..., description="Wikibase instance URL")
    username: str | None = Field(None, description="Username for authentication")
    password: str | None = Field(None, description="Password for authentication")
    token: str | None = Field(None, description="API token for authentication")
    entity_namespace: str = Field("Item", description="Entity namespace (Item, Property)")
    property_namespace: str = Field("Property", description="Property namespace")
    sparql_endpoint: str | None = Field(None, description="SPARQL endpoint URL")


class ProjectConfig(BaseModel):
    """Main project configuration."""
    
    name: str = Field(..., description="Project name")
    version: str = Field("1.0.0", description="Project version")
    description: str | None = Field(None, description="Project description")
    
    wikibase: WikibaseConfig = Field(..., description="Wikibase configuration")
    
    # Additional settings
    settings: dict[str, Any] = Field(default_factory=dict, description="Additional settings")
