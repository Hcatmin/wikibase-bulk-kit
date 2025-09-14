"""Schema synchronization for Wikibase properties and items."""

from .sync import SchemaSyncer
from .models import PropertySchema, ItemSchema, SchemaConfig

__all__ = ["SchemaSyncer", "PropertySchema", "ItemSchema", "SchemaConfig"]
