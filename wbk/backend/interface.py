from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..schema import PropertySchema, ItemSchema

class BackendStrategy(ABC):
    """Abstract base class for Wikibase backend strategies."""
    def __init__(self, language: str):
        self.language = language

    @abstractmethod
    def find_property_by_label(self, label: str) -> Optional[str]:
        """Find a property by exact label match."""
        pass

    @abstractmethod
    def find_item_by_label(self, label: str) -> Optional[str]:
        """Find an item by exact label match."""
        pass
    
    @abstractmethod
    def find_item_by_label_and_description(self, label: str, description: str) -> Optional[str]:
        """Find an item by exact label and description match."""
        pass

    @abstractmethod
    def find_item_by_expression(self, expression: str) -> Optional[str]:
        """Find an item by expression (label or label (description))."""
        pass

    @abstractmethod
    def create_property(self, property_schema: PropertySchema) -> Optional[str]:
        """Create a new property in Wikibase."""
        pass

    @abstractmethod
    def update_property(self, property_schema: PropertySchema) -> bool:
        """Update an existing property in Wikibase."""
        pass

    @abstractmethod
    def create_item(self, item_schema: ItemSchema) -> Optional[str]:
        """Create a new item in Wikibase."""
        pass

    @abstractmethod
    def update_item(self, item_schema: ItemSchema) -> bool:
        """Update an existing item in Wikibase."""
        pass

    @abstractmethod
    def find_qids(self, keys: List[dict]) -> dict:
        """
        Bulk find QIDs for a list of keys.
        keys: List of dicts, e.g. [{'label': 'foo', 'unique_key': {'property': 'P1', 'value': '123'}}]
        Returns: Dict of QIDs by (label, unique_key_value)
        """
        pass

    @abstractmethod
    def create_items(self, items: List[dict]) -> List[str]:
        """
        Bulk create items.
        items: List of item dictionaries (RaiseWikibase format or similar).
        Returns: List of created QIDs.
        """
        pass

    @abstractmethod
    def update_items(self, items: List[dict]) -> List[bool]:
        """
        Bulk update items.
        items: List of item dictionaries (RaiseWikibase format or similar).
        Returns: List of success booleans.
        """
        pass
