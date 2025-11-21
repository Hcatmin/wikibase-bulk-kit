"""Shared context and caching utilities for the mapping pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from RaiseWikibase.dbconnection import DBConnection
from wbk.processor.bulk_item_search import ItemBulkSearcher

from ..models import StatementMapping, ClaimMapping


def _iter_claims(schema: list[ClaimMapping] | list[StatementMapping] | None):
    if not schema:
        return []
    return [claim for claim in schema if getattr(claim, "property_label", None)]


@dataclass
class MappingContext:
    """Holds cross-cutting collaborators for the mapping pipeline."""

    language: str
    pid_cache: dict[str, str] = field(default_factory=dict)
    qid_cache: dict[str | tuple[str, str | None], str] = field(default_factory=dict)
    db_connection: DBConnection = field(default_factory=DBConnection)

    def ensure_pids(self, statements: list[StatementMapping] | None) -> None:
        """Populate the PID cache using property labels found in statements."""

        def _maybe_add(property_label: str | None):
            if not property_label or property_label in self.pid_cache:
                return
            self.pid_cache[property_label] = (
                self.db_connection.find_property_id(property_label)
            )

        if not statements:
            return

        for statement in statements:
            _maybe_add(statement.property_label)
            for qualifier in _iter_claims(statement.qualifiers):
                _maybe_add(qualifier.property_label)
            for reference in _iter_claims(statement.references):
                _maybe_add(reference.property_label)

    def ensure_qids(
        self,
        label_description_pairs: Iterable[tuple[str, str | None]],
    ) -> None:
        """Populate the QID cache for the provided value statements."""
        with ItemBulkSearcher() as item_searcher:
            qids_found = item_searcher.find_qids(label_description_pairs)

        self.qid_cache.update(qids_found)
        
    def get_property_id(
        self,
        property_id: str | None,
        property_label: str | None,
    ) -> str:
        """Resolve a property identifier from an id or label."""
        if property_id:
            return property_id
        if property_label and property_label in self.pid_cache:
            return self.pid_cache[property_label]
        raise ValueError("Property id or property label is required")

    def get_qid(self, label_or_qid: str | tuple[str, str | None]) -> str | None:
        """Return a QID for a provided label/description pair or QID literal."""
        if isinstance(label_or_qid, tuple):
            label_value, description_value = label_or_qid
            normalized_label = self._normalize_term(label_value)
            normalized_description = self._normalize_term(description_value)
            if not normalized_label:
                return None
            return self.qid_cache.get((normalized_label, normalized_description)) or self.qid_cache.get(normalized_label)

        if (
            label_or_qid.startswith("Q")
            and len(label_or_qid) > 1
            and label_or_qid[1:].isdigit()
        ):
            return label_or_qid
        return self.qid_cache.get((label_or_qid, None))

    def verify_items_created(self) -> None:
        """Run a light-weight verification against the DBConnection."""
        try:
            latest_eid = self.db_connection.get_last_eid(content_model="wikibase-item")
            print(f"Latest item ID in database: Q{latest_eid}")

            cursor = self.db_connection.conn.cursor()
            cursor.execute(
                """
                SELECT page_title, page_id
                FROM page
                WHERE page_namespace = 0
                ORDER BY page_id DESC
                LIMIT 10
                """
            )
            recent_items = cursor.fetchall()
            print(f"Recent items in database: {recent_items}")
        except Exception as exc:
            print(f"Warning: Could not verify items in database: {exc}")

    @staticmethod
    def _normalize_term(term: str | None) -> str | None:
        if term is None:
            return None
        term_str = str(term).strip()
        return term_str or None
