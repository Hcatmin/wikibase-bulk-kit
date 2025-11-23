from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Tuple

from ..schema.models import PropertySchema, ItemSchema, StatementSchema, ClaimSchema

class BackendStrategy(ABC):
    """Abstract base class for Wikibase backend strategies."""

    @abstractmethod
    def find_property_by_label(self, label: str, language: str) -> Optional[str]:
        """Find a property by exact label match."""
        pass

    @abstractmethod
    def find_item_by_label(self, label: str, language: str) -> Optional[str]:
        """Find an item by exact label match."""
        pass
    
    @abstractmethod
    def find_item_by_label_and_description(self, label: str, description: str, language: str) -> Optional[str]:
        """Find an item by exact label and description match."""
        pass

    @abstractmethod
    def find_item_by_expression(self, expression: str, language: str) -> Optional[str]:
        """Find an item by expression (label or label (description))."""
        pass

    @abstractmethod
    def find_items_by_labels(self, labels: List[str], language: str) -> Dict[str, Optional[str]]:
        """Find items by a list of labels."""
        pass

    @abstractmethod
    def find_items_by_keys(self, keys: List[Tuple[str, Optional[str]]], language: str) -> Dict[Tuple[str, Optional[str]], Optional[str]]:
        """Find items by (label, description) keys."""
        pass

    @abstractmethod
    def find_items_with_data(self, keys: List[Tuple[str, Optional[str]]], language: str) -> Dict[Tuple[str, Optional[str]], dict]:
        """Find items with full data by (label, description) keys."""
        pass

    @abstractmethod
    def create_property(self, property_schema: PropertySchema, language: str) -> Optional[str]:
        """Create a new property in Wikibase."""
        pass

    @abstractmethod
    def update_property(self, property_schema: PropertySchema, language: str) -> bool:
        """Update an existing property in Wikibase."""
        pass

    @abstractmethod
    def create_item(self, item_schema: ItemSchema, language: str) -> Optional[str]:
        """Create a new item in Wikibase."""
        pass

    @abstractmethod
    def update_item(self, item_schema: ItemSchema, language: str) -> bool:
        """Update an existing item in Wikibase."""
        pass
