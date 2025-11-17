"""Utility helpers to resolve values and infer required columns."""

from __future__ import annotations

from typing import Any

import pandas as pd

from pydantic.types import Any as PydanticAny  # align with existing type hints

from ..models import ValueSpec
from .context import MappingContext


class ValueResolver:
    """Parses `ValueSpec` structures and resolves them to Wikibase values."""

    def extract_columns(self, value_spec: ValueSpec | None) -> list[str]:
        columns: list[str] = []
        if value_spec is None:
            return columns

        if isinstance(value_spec, str):
            columns.append(value_spec)
        elif isinstance(value_spec, dict):
            column_name = value_spec.get("column")
            if column_name:
                columns.append(column_name)
        elif isinstance(value_spec, list):
            for elem in value_spec:
                columns.extend(self.extract_columns(elem))
        return columns

    def extract_labels(
        self,
        value_spec: ValueSpec | None,
        datatype: str,
        dataframe: pd.DataFrame | None = None,
    ) -> list[str]:
        if value_spec is None or datatype != "wikibase-item":
            return []

        labels: list[str] = []
        if isinstance(value_spec, str):
            if dataframe is not None and value_spec in dataframe.columns:
                labels.extend(
                    dataframe[value_spec].drop_duplicates().dropna().tolist()
                )
        elif isinstance(value_spec, dict):
            if "column" in value_spec:
                column = value_spec["column"]
                if dataframe is not None and column in dataframe.columns:
                    labels.extend(
                        dataframe[column].drop_duplicates().dropna().tolist()
                    )
            elif "label" in value_spec:
                labels.append(value_spec["label"])
            elif "value" in value_spec:
                static_value = value_spec["value"]
                if isinstance(static_value, str) and not (
                    static_value.startswith("Q") and static_value[1:].isdigit()
                ):
                    labels.append(static_value)
        elif isinstance(value_spec, list):
            for elem in value_spec:
                labels.extend(self.extract_labels(elem, datatype, dataframe))
        return labels

    def resolve(
        self,
        value_spec: ValueSpec | None,
        row: pd.Series,
        datatype: str,
        context: MappingContext,
    ) -> Any | tuple:
        if value_spec is None:
            raise ValueError(f"No value specified for datatype {datatype}")

        def resolve_element(elem: ValueSpec) -> PydanticAny:
            if isinstance(elem, str):
                return row[elem]
            if isinstance(elem, dict):
                if "column" in elem:
                    return row[elem["column"]]
                if "value" in elem:
                    return elem["value"]
                if "label" in elem:
                    if datatype == "wikibase-item":
                        qid = context.get_qid(elem["label"])
                        if qid:
                            return qid
                    return elem["label"]
                raise ValueError(
                    f"Invalid value spec dict: {elem}. "
                    "Must have 'column', 'value', or 'label'."
                )
            if isinstance(elem, list):
                return tuple(resolve_element(sub) for sub in elem)
            raise ValueError(f"Invalid value spec element: {elem}")

        if isinstance(value_spec, list):
            return tuple(resolve_element(elem) for elem in value_spec)

        resolved = resolve_element(value_spec)
        if datatype == "wikibase-item" and isinstance(resolved, str):
            qid = context.get_qid(resolved)
            if qid:
                return qid
        return resolved
