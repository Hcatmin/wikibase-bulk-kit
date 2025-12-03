"""
RaiseWikibase - A Python library for direct database operations on Wikibase
instances.

This package provides tools for creating and managing Wikibase entities
through direct database connections, bypassing the MediaWiki API for
high-performance bulk operations.
"""

from RaiseWikibase.api import Session
from RaiseWikibase.raiser import (
    page,
    batch,
    create_bot,
    building_indexing,
)
from RaiseWikibase.datamodel import (
    label,
    alias,
    description,
    snak,
    claim,
    mainsnak,
    statement,
    entity,
    namespaces,
    datatypes,
)
from RaiseWikibase.dbconnection import DBConnection
from RaiseWikibase.settings import Settings

__version__ = "0.1.0"

__all__ = [
    "Session",
    "page",
    "batch",
    "create_bot",
    "building_indexing",
    "label",
    "alias",
    "description",
    "snak",
    "claim",
    "mainsnak",
    "statement",
    "entity",
    "namespaces",
    "datatypes",
    "DBConnection",
    "Settings",
]
