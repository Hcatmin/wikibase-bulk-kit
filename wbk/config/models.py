"""Pydantic models for configuration validation."""

from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class WikibaseConfig(BaseModel):
    """Wikibase instance configuration."""
    
    url: str = Field(..., description="Wikibase instance URL")
    username: Optional[str] = Field(None, description="Username for authentication")
    password: Optional[str] = Field(None, description="Password for authentication")
    token: Optional[str] = Field(None, description="API token for authentication")
    entity_namespace: str = Field("Item", description="Entity namespace (Item, Property)")
    property_namespace: str = Field("Property", description="Property namespace")


class ProjectConfig(BaseModel):
    """Main project configuration."""
    
    name: str = Field(..., description="Project name")
    version: str = Field("1.0.0", description="Project version")
    description: Optional[str] = Field(None, description="Project description")
    
    wikibase: WikibaseConfig = Field(..., description="Wikibase configuration")
    
    # Additional settings
    settings: Dict[str, Any] = Field(default_factory=dict, description="Additional settings")
