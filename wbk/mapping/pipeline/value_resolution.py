"""Utility helpers to resolve values and infer required columns."""

from __future__ import annotations

from typing import Any
import re

import pandas as pd

from pydantic.types import Any as PydanticAny  # align with existing type hints

from ..models import ValueSpec
from .context import MappingContext
import wbk.mapping.utils as utils


class ValueResolver:
    """Parses `ValueSpec` structures and resolves them to Wikibase values."""

    def extract_columns(self, value_spec: ValueSpec | None) -> list[str]:
        columns: list[str] = []
        if value_spec is None:
            return columns

        if isinstance(value_spec, str):
            columns.append(value_spec)
        elif isinstance(value_spec, dict):
            column_name = value_spec.get("column") or value_spec.get("value_column")
            if column_name:
                columns.append(column_name)
            description_column = value_spec.get("description_column")
            if description_column:
                columns.append(description_column)
            columns.extend(self._extract_description_columns(value_spec.get("value_description")))
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

    def extract_label_description_pairs(
        self,
        value_spec: ValueSpec | None,
        dataframe: pd.DataFrame | None = None,
    ) -> list[tuple[str, str | None]]:
        """Collect (label, description) pairs used for wikibase-item values."""
        pairs: set[tuple[str, str | None]] = set()
        if value_spec is None:
            return []

        def add_pair(label_value, description_value):
            label_text = self._normalize_value(label_value)
            if not label_text or self._looks_like_qid(label_text):
                return
            description_text = self._normalize_value(description_value)
            pairs.add((label_text, description_text))

        def handle_dict(spec: dict):
            column_name = spec.get("column") or spec.get("value_column")
            (
                description_column,
                static_description,
                template_info,
            ) = self._resolve_description_sources(spec, dataframe)

            if column_name and dataframe is not None and column_name in dataframe.columns:
                columns_to_fetch: list[str] = [column_name]
                if description_column:
                    columns_to_fetch.append(description_column)
                if template_info:
                    _, template_columns = template_info
                    columns_to_fetch.extend(template_columns)
                columns_to_fetch = [
                    col
                    for col in dict.fromkeys(columns_to_fetch)
                    if col and col in dataframe.columns
                ]
                if not columns_to_fetch:
                    return
                subset = dataframe[columns_to_fetch].dropna(subset=[column_name])
                for _, row in subset.iterrows():
                    if description_column:
                        description_value = row.get(description_column)
                    elif template_info:
                        description_value = self._render_template(
                            template_info[0],
                            template_info[1],
                            row,
                        )
                    else:
                        description_value = static_description
                    add_pair(row[column_name], description_value)
            elif "label" in spec:
                description_value = static_description
                if template_info and dataframe is not None:
                    # template without a column is ambiguous; treat placeholders as empty
                    description_value = self._render_template(
                        template_info[0],
                        template_info[1],
                        pd.Series(dtype=object),
                    )
                add_pair(spec["label"], description_value)
            elif "value" in spec:
                description_value = static_description
                add_pair(spec["value"], description_value)

        if isinstance(value_spec, str):
            if dataframe is not None and value_spec in dataframe.columns:
                series = dataframe[value_spec].dropna()
                for label_value in series:
                    add_pair(label_value, None)
            else:
                add_pair(value_spec, None)
        elif isinstance(value_spec, dict):
            handle_dict(value_spec)
        elif isinstance(value_spec, list):
            for elem in value_spec:
                if isinstance(elem, dict):
                    handle_dict(elem)
                elif isinstance(elem, str):
                    if dataframe is not None and elem in dataframe.columns:
                        series = dataframe[elem].dropna()
                        for label_value in series:
                            add_pair(label_value, None)
                    else:
                        add_pair(elem, None)
        return list(pairs)

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
                    if "value_description" in elem:
                        qid = context.get_qid((row[elem["column"]], utils.create_description_row(row, elem["value_description"])))
                        if qid:
                            return qid
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

    @staticmethod
    def _looks_like_qid(value: str) -> bool:
        return (
            isinstance(value, str)
            and value.startswith("Q")
            and len(value) > 1
            and value[1:].isdigit()
        )

    @staticmethod
    def _normalize_value(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            normalized = value.strip()
        else:
            try:
                if pd.isna(value):  # type: ignore[attr-defined]
                    return None
            except TypeError:
                pass
            normalized = str(value).strip()
        return normalized or None

    def _parse_description_spec(
        self,
        description_spec: ValueSpec | None,
        dataframe: pd.DataFrame | None = None,
    ) -> tuple[str | None, str | None, tuple[str, list[str]] | None]:
        if description_spec is None:
            return None, None, None

        if isinstance(description_spec, str):
            template_columns = self._extract_template_columns(description_spec)
            if template_columns:
                return None, None, (description_spec, template_columns)
            if dataframe is not None and description_spec in dataframe.columns:
                return description_spec, None, None
            return None, description_spec, None

        if isinstance(description_spec, dict):
            if "column" in description_spec:
                return description_spec["column"], None, None
            if "value" in description_spec:
                value = description_spec["value"]
                if isinstance(value, str):
                    template_columns = self._extract_template_columns(value)
                    if template_columns:
                        return None, None, (value, template_columns)
                return None, value, None
            if "label" in description_spec:
                label = description_spec["label"]
                if isinstance(label, str):
                    template_columns = self._extract_template_columns(label)
                    if template_columns:
                        return None, None, (label, template_columns)
                return None, label, None

        if isinstance(description_spec, list):
            column_name: str | None = None
            static_value: str | None = None
            template_info: tuple[str, list[str]] | None = None
            for elem in description_spec:
                col, static, template = self._parse_description_spec(elem, dataframe)
                if template:
                    return None, None, template
                if col and not column_name:
                    column_name = col
                if static and not static_value:
                    static_value = static
            return column_name, static_value, template_info

        return None, None, None

    def _extract_description_columns(self, description_spec: ValueSpec | None) -> list[str]:
        columns: list[str] = []
        if description_spec is None:
            return columns
        if isinstance(description_spec, str):
            columns.extend(self._extract_template_columns(description_spec))
        elif isinstance(description_spec, dict):
            if "column" in description_spec:
                columns.append(description_spec["column"])
            elif "value" in description_spec and isinstance(description_spec["value"], str):
                columns.extend(self._extract_template_columns(description_spec["value"]))
            elif "label" in description_spec and isinstance(description_spec["label"], str):
                columns.extend(self._extract_template_columns(description_spec["label"]))
        elif isinstance(description_spec, list):
            for elem in description_spec:
                columns.extend(self._extract_description_columns(elem))
        return columns

    @staticmethod
    def _extract_template_columns(template: str) -> list[str]:
        if not template or "{" not in template:
            return []
        return list(dict.fromkeys(re.findall(r"{(.*?)}", template)))

    def _resolve_description_sources(
        self,
        spec: dict,
        dataframe: pd.DataFrame | None,
    ) -> tuple[str | None, str | None, tuple[str, list[str]] | None]:
        description_column = spec.get("description_column")
        static_description = spec.get("description_value")
        template_info: tuple[str, list[str]] | None = None

        def merge(description_spec: ValueSpec | None):
            nonlocal description_column, static_description, template_info
            if description_spec is None:
                return
            col, static, template = self._parse_description_spec(description_spec, dataframe)
            if col and not description_column:
                description_column = col
            if template:
                template_info = template
            elif static and static_description is None:
                static_description = static

        merge(spec.get("value_description"))
        merge(spec.get("description"))
        merge(spec.get("description_value"))

        return description_column, static_description, template_info

    def _render_template(
        self,
        template: str,
        columns: list[str],
        row: pd.Series,
    ) -> str:
        result = template
        for column in columns:
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
                    replacement = str(value) if value is not None else ""
            result = result.replace(f"{{{column}}}", replacement)
        return result
