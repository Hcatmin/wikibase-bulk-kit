"""Utility helpers to resolve values and infer required columns."""

from __future__ import annotations

from typing import Any, Iterable, Tuple, Set
import re

import pandas as pd

from ..models import ValueSpec, ValueDefinition
from .context import MappingContext


class ValueResolver:
    """Parses `ValueSpec` structures and resolves them to concrete values."""

    def extract_columns(self, value_spec: ValueSpec | None) -> list[str]:
        """Return dataframe columns required to resolve a value spec."""
        columns: list[str] = []
        if value_spec is None:
            return columns

        if isinstance(value_spec, ValueDefinition):
            if value_spec.label:
                columns.extend(self._extract_template_columns(value_spec.label))
                columns.append(value_spec.label)
            if value_spec.snak and value_spec.snak.value:
                columns.extend(self._extract_template_columns(value_spec.snak.value))
                columns.append(value_spec.snak.value)
        elif isinstance(value_spec, str):
            columns.append(value_spec)
            columns.extend(self._extract_template_columns(value_spec))
        elif isinstance(value_spec, dict):
            column_name = value_spec.get("column") or value_spec.get("value_column")
            if column_name:
                columns.append(column_name)
            if "value" in value_spec and isinstance(value_spec["value"], str):
                columns.extend(self._extract_template_columns(value_spec["value"]))
            if "label" in value_spec and isinstance(value_spec["label"], str):
                columns.extend(self._extract_template_columns(value_spec["label"]))
        elif isinstance(value_spec, list):
            for elem in value_spec:
                columns.extend(self.extract_columns(elem))
        return list(dict.fromkeys(columns))

    def extract_item_lookups(
        self,
        value_spec: ValueSpec | None,
        dataframe: pd.DataFrame | None = None,
    ) -> tuple[Set[str], Set[Tuple[str, str, str]]]:
        """
        Collect label-only and unique-key-based lookup requests for wikibase-item values.

        Returns:
            (label_only, unique_keys) where:
            - label_only: set of labels to resolve by label only
            - unique_keys: set of tuples (label, unique_property_label, unique_value)
        """
        label_only: set[str] = set()
        unique_keys: set[tuple[str, str, str]] = set()

        def add_labels_from_series(template: str | None):
            if template is None:
                return set()
            if dataframe is not None and template in dataframe.columns:
                return set(
                    dataframe[template]
                    .dropna()
                    .astype(str)
                    .map(str.strip)
                    .tolist()
                )
            return {
                val
                for val in self._resolve_series_from_template(template, dataframe)
                if val
            }

        if value_spec is None:
            return label_only, unique_keys

        if isinstance(value_spec, ValueDefinition):
            labels = add_labels_from_series(value_spec.label)
            if value_spec.snak:
                values = self._resolve_series_from_template(
                    value_spec.snak.value, dataframe
                )
                for lbl in labels or {None}:
                    for val in values:
                        if lbl and val:
                            unique_keys.add(
                                (
                                    lbl,
                                    value_spec.snak.property,
                                    val,
                                )
                            )
            else:
                label_only.update(lbl for lbl in labels if lbl)
        elif isinstance(value_spec, str):
            label_only.update(add_labels_from_series(value_spec))
        elif isinstance(value_spec, dict):
            col = value_spec.get("column") or value_spec.get("value_column")
            if col:
                label_only.update(add_labels_from_series(col))
            elif "label" in value_spec:
                label_only.add(str(value_spec["label"]))
            elif "value" in value_spec:
                label_only.add(str(value_spec["value"]))
        elif isinstance(value_spec, list):
            for elem in value_spec:
                l_only, u_keys = self.extract_item_lookups(elem, dataframe)
                label_only.update(l_only)
                unique_keys.update(u_keys)
        return label_only, unique_keys

    def resolve(
        self,
        value_spec: ValueSpec | None,
        row: pd.Series,
        datatype: str | None,
        context: MappingContext,
    ) -> Any | tuple:
        """Resolve a value specification using a dataframe row and context caches."""
        if value_spec is None:
            raise ValueError("No value specified")

        def resolve_element(elem: ValueSpec):
            if isinstance(elem, ValueDefinition):
                label_value = self._render_template(elem.label, row) if elem.label else None
                if elem.snak:
                    unique_value = self._render_template(elem.snak.value, row)
                    return context.get_qid_by_unique_key(
                        label_value,
                        elem.snak.property,
                        unique_value,
                    )
                if datatype == "wikibase-item":
                    qid = context.get_qid_by_label(label_value)
                    return qid or label_value
                return label_value

            if isinstance(elem, str):
                if elem in row.index:
                    raw = row[elem]
                elif "{" in elem:
                    raw = self._render_template(elem, row)
                else:
                    raw = elem
                if datatype == "wikibase-item":
                    qid = context.get_qid_by_label(raw)
                    return qid or raw
                return raw

            if isinstance(elem, dict):
                if "column" in elem:
                    return row.get(elem["column"])
                if "value" in elem:
                    val = elem["value"]
                    if isinstance(val, str) and "{" in val:
                        return self._render_template(val, row)
                    return val
                if "label" in elem:
                    lbl = elem["label"]
                    if isinstance(lbl, str) and "{" in lbl:
                        lbl = self._render_template(lbl, row)
                    if datatype == "wikibase-item":
                        qid = context.get_qid_by_label(lbl)
                        return qid or lbl
                    return lbl
                raise ValueError(f"Invalid value spec dict: {elem}")

            if isinstance(elem, list):
                return tuple(resolve_element(sub) for sub in elem)

            return elem

        if isinstance(value_spec, list):
            return tuple(resolve_element(elem) for elem in value_spec)

        resolved = resolve_element(value_spec)
        if (
            datatype == "wikibase-item"
            and isinstance(resolved, str)
            and resolved.startswith("Q")
            and resolved[1:].isdigit()
        ):
            return resolved
        return resolved

    def _resolve_series_from_template(
        self,
        template: str | None,
        dataframe: pd.DataFrame | None,
    ) -> set[str]:
        """Resolve a template over an entire dataframe column-wise, returning unique non-empty values."""
        if template is None:
            return set()
        columns = self._extract_template_columns(template)
        if dataframe is not None and template in dataframe.columns:
            return set(
                dataframe[template]
                .dropna()
                .astype(str)
                .map(str.strip)
                .tolist()
            )
        if not columns:
            return {template}
        if dataframe is None:
            return set()
        available_cols = [col for col in columns if col in dataframe.columns]
        if not available_cols:
            return set()
        resolved: set[str] = set()
        for _, row in dataframe[available_cols].dropna().iterrows():
            resolved_value = self._render_template(template, row)
            if resolved_value:
                resolved.add(resolved_value)
        return resolved

    @staticmethod
    def _render_template(template: str | None, row: pd.Series) -> str | None:
        if not template:
            return None
        result = template
        for column in set(re.findall(r"{(.*?)}", template)):
            replacement = ""
            if column in row:
                value = row[column]
                if isinstance(value, str):
                    replacement = value.strip()
                else:
                    try:
                        if pd.isna(value):  # type: ignore[attr-defined]
                            value = ""
                    except TypeError:
                        pass
                    replacement = "" if value is None else str(value)
            result = result.replace(f"{{{column}}}", replacement)
        return result

    @staticmethod
    def _extract_template_columns(template: str) -> list[str]:
        if not template or "{" not in template:
            return []
        return list(dict.fromkeys(re.findall(r"{(.*?)}", template)))
