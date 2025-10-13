"""Mapping layer for transforming CSV data into Wikibase statements.

This module provides functionality to:
- Load CSV files and apply column mappings
- Transform CSV data into Wikibase statement objects
- Create statements with qualifiers, references, and ranks
- Export results in various formats
"""

from .models import (
    CSVFileConfig,
    ItemMapping,
    MappingConfig,
    StatementMapping,
    ClaimMapping
)
from .processor import MappingProcessor

__all__ = [
    # Models
    "ClaimMapping",
    "CSVFileConfig", 
    "ItemMapping",
    "MappingConfig",
    "StatementMapping",
    # Processor
    "MappingProcessor",
]

