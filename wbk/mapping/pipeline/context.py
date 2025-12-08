"""Shared context and caching utilities for the mapping pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Tuple, Dict, Any

from RaiseWikibase.dbconnection import DBConnection
from wbk.backend.raisewikibase import RaiseWikibaseBackend

from ..models import StatementDefinition, ValueDefinition, MappingRule


def _iter_claims(schema: list[StatementDefinition] | None):
    if not schema:
        return []
    return [claim for claim in schema if getattr(claim, "property", None)]


@dataclass
class MappingContext:
    """Holds cross-cutting collaborators for the mapping pipeline."""

    def __init__(self, language: str) -> None:
        self.language = language
        # property label -> {"id": pid, "datatype": datatype}
        self.pid_cache: dict[str, Dict[str, str | None]] = {}
        # label -> qid cache (label-only lookups)
        self.qid_cache_label: dict[str, str] = {}
        # (label, property_id, unique_value) -> qid
        self.qid_cache_unique: dict[tuple[str, str, str], str] = {}
        self.db_connection = DBConnection()
        self.item_searcher = RaiseWikibaseBackend()

    def ensure_properties(self, mapping: MappingRule) -> None:
        """Populate the property cache (id + datatype) using labels found in statements and unique keys."""
        labels: set[str] = set()

        statements = mapping.statements
        unique_keys = [mapping.item.unique_key] if mapping.item.unique_key else None

        def _collect_unique_key_from_value(value_spec: Any):
            """Recursively collect property labels from ValueDefinition.unique_key."""
            if isinstance(value_spec, ValueDefinition):
                if value_spec.unique_key and value_spec.unique_key.property:
                    labels.add(value_spec.unique_key.property)
                return
            if isinstance(value_spec, list):
                for elem in value_spec:
                    _collect_unique_key_from_value(elem)

        def _collect_from_statement(statement: StatementDefinition):
            labels.add(statement.property)
            _collect_unique_key_from_value(statement.value)
            for qualifier in _iter_claims(statement.qualifiers):
                labels.add(qualifier.property)
                _collect_unique_key_from_value(qualifier.value)
            for reference in _iter_claims(statement.references):
                labels.add(reference.property)
                _collect_unique_key_from_value(reference.value)

        if statements:
            for stmt in statements:
                _collect_from_statement(stmt)

        if unique_keys:
            for uk in unique_keys:
                if uk and uk.property:
                    labels.add(uk.property)

        for label in labels:
            if label in self.pid_cache:
                continue
            pid, datatype = self.db_connection.find_property_info(label)
            if not pid:
                raise ValueError(f"Property not found: {label}")
            self.pid_cache[label] = {"id": pid, "datatype": datatype}

    def ensure_qids_for_labels(self, labels: Iterable[str]) -> None:
        """Populate qid cache for label-only wikibase-item references."""
        normalized = [self._normalize_term(lbl) for lbl in labels]
        normalized = [lbl for lbl in normalized if lbl]
        if not normalized:
            return
        qids_found = self.item_searcher.find_items_by_labels_optimized(
            list(dict.fromkeys(normalized))
        )
        for label, qid in qids_found.items():
            if qid:
                self.qid_cache_label[label] = qid

    def ensure_qids_for_unique_keys(
        self,
        keys: Iterable[Tuple[str, str]],
        property_id: str,
        property_datatype: str | None,
    ) -> None:
        """Populate qid cache for label + unique_key combinations."""
        normalized_keys = []
        for label, value in keys:
            norm_label = self._normalize_term(label)
            norm_value = self._normalize_unique_value(value, property_datatype)
            if norm_label and norm_value:
                normalized_keys.append((norm_label, norm_value))
        if not normalized_keys:
            return
        items_found = self.item_searcher.find_items_by_unique_key(
            normalized_keys,
            property_id=property_id,
            property_datatype=property_datatype,
            language=self.language,
        )
        for (label, value), item in items_found.items():
            qid = item.get("id") if item else None
            if qid:
                norm_label = self._normalize_term(label)
                norm_value = self._normalize_unique_value(value, property_datatype)
                if norm_label and norm_value:
                    self.qid_cache_unique[(norm_label, property_id, norm_value)] = qid

    def get_property_info(self, property_label_or_id: str) -> tuple[str, str | None]:
        """Resolve property id and datatype by label or id."""
        if property_label_or_id in self.pid_cache:
            info = self.pid_cache[property_label_or_id]
            return info["id"], info.get("datatype")

        pid, datatype = self.db_connection.find_property_info(property_label_or_id)
        if pid and property_label_or_id:
            self.pid_cache[property_label_or_id] = {"id": pid, "datatype": datatype}
        return pid, datatype

    def get_property_id(self, property_label_or_id: str | None) -> str:
        """Resolve a property identifier from an id or label."""
        if not property_label_or_id:
            raise ValueError("Property label or id is required")
        pid, _ = self.get_property_info(property_label_or_id)
        if not pid:
            raise ValueError(f"Property not found: {property_label_or_id}")
        return pid

    def get_property_datatype(self, property_label_or_id: str) -> str | None:
        """Return cached property datatype."""
        _, datatype = self.get_property_info(property_label_or_id)
        return datatype

    def get_qid_by_label(self, label: str | None) -> str | None:
        """Return qid for label-only lookup."""
        norm = self._normalize_term(label)
        if not norm:
            return None
        if norm in self.qid_cache_label:
            return self.qid_cache_label[norm]
        return None

    def get_qid_by_unique_key(
        self,
        label: str | None,
        property_label_or_id: str,
        value: str | None,
    ) -> str | None:
        """Return qid for a label + unique key combination (property/value)."""
        norm_label = self._normalize_term(label)
        property_id, property_datatype = self.get_property_info(property_label_or_id)
        norm_value = self._normalize_unique_value(value, property_datatype)
        if not norm_label or not norm_value:
            return None

        cached = self.qid_cache_unique.get((norm_label, property_id, norm_value))
        if cached:
            return cached

        # Fallback to on-demand lookup
        found_items = self.item_searcher.find_items_by_unique_key(
            [(norm_label, norm_value)],
            property_id=property_id,
            property_datatype=self.get_property_datatype(property_label_or_id),
            language=self.language,
        )
        item = found_items.get((norm_label, norm_value))
        if item:
            qid = item.get("id")
            self.qid_cache_unique[(norm_label, property_id, norm_value)] = qid
            return qid
        return None

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

    @staticmethod
    def _normalize_unique_value(value: Any | None, datatype: str | None) -> str | None:
        """Normalize unique key values consistently with item search."""
        if value is None:
            return None

        normalized = value
        if isinstance(normalized, dict):
            normalized = (
                normalized.get("amount")
                or normalized.get("value")
                or normalized.get("text")
                or normalized.get("id")
                or normalized
            )

        if datatype == "quantity":
            try:
                if isinstance(normalized, str) and normalized.startswith("+"):
                    normalized = normalized[1:]
                numeric = float(str(normalized))
                if numeric.is_integer():
                    normalized = str(int(numeric))
                else:
                    normalized = str(numeric)
            except Exception:
                normalized = str(normalized)
        else:
            normalized = str(normalized)

        normalized = normalized.strip()
        return normalized or None
