"""New mapping processor implementation using a composable pipeline."""

from __future__ import annotations

from pathlib import Path
import yaml
import pandas as pd

from ..config.manager import ConfigManager
from wbk.mapping.models import (
    MappingRule,
    CSVFileConfig,
    MappingConfig,
)
from .pipeline import (
    MappingContext,
    ValueResolver,
    ClaimBuilder,
    UpdateStrategy,
    UpdateStrategyFactory,
    CreateItemsStep,
)

from wbk.processor.bulk_item_search import ItemBulkSearcher


class MappingProcessor:
    """Processes mapping configurations using the new pipeline architecture."""

    def __init__(self, config_manager: ConfigManager, chunk_size: int = 1000) -> None:
        self.config_manager = config_manager
        self.chunk_size = chunk_size
        self.value_resolver = ValueResolver()
        self.claim_builder = ClaimBuilder(self.value_resolver)
        self.creator = CreateItemsStep(
            claim_builder=self.claim_builder,
        )
        self.updater: UpdateStrategy | None = None

    def _load_mapping_config(self, mapping_path: str) -> MappingConfig:
        mapping_file = Path(mapping_path)
        if not mapping_file.exists():
            raise FileNotFoundError(f"Mapping file not found: {mapping_path}")

        with open(mapping_file, "r", encoding="utf-8") as file_handle:
            mapping_data = yaml.safe_load(file_handle)

        return MappingConfig(**mapping_data)

    def _load_dataframe(
        self,
        csv_config: CSVFileConfig,
        mapping_config: MappingConfig,
    ) -> pd.DataFrame:
        encoding = csv_config.encoding or mapping_config.encoding
        delimiter = csv_config.delimiter or mapping_config.delimiter
        decimal_separator = csv_config.decimal_separator or mapping_config.decimal_separator
        return pd.read_csv(
            csv_config.file_path,
            encoding=encoding,
            delimiter=delimiter,
            decimal=decimal_separator,
        )

    @staticmethod
    def _clean_value(value: object) -> object | None:
        """Normalize scalar values from a dataframe row (trim strings, drop NaN)."""
        try:
            if value is None or pd.isna(value):  # type: ignore[attr-defined]
                return None
        except Exception:
            if value is None:
                return None

        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value

    def _render_value(self, template: str | None, row: pd.Series) -> object | None:
        """Render a template or column name against a dataframe row."""
        if template is None:
            return None
        if "{" in template:
            return self._clean_value(self.value_resolver._render_template(template, row))
        if template in row.index:
            return self._clean_value(row.get(template))
        return self._clean_value(template)

    def _extract_template_columns(self, template: str | None) -> set[str]:
        if not template:
            return set()
        return set(self.value_resolver._extract_template_columns(template))

    def _required_columns(
        self,
        mapping_rule: MappingRule,
    ) -> list[str]:
        """Collect dataframe columns needed to resolve item and statement values."""
        columns: set[str] = set()
        item_def = mapping_rule.item

        # Item label/unique key/description templates
        columns.update(self._extract_template_columns(item_def.label))
        if "{" not in item_def.label and item_def.label:
            columns.add(item_def.label)

        if item_def.unique_key and item_def.unique_key.value:
            columns.update(self._extract_template_columns(item_def.unique_key.value))
            if "{" not in item_def.unique_key.value:
                columns.add(item_def.unique_key.value)

        if item_def.description:
            columns.update(self._extract_template_columns(item_def.description))
            if "{" not in item_def.description:
                columns.add(item_def.description)

        def extend_with_statement(statement):
            columns.update(self.value_resolver.extract_columns(statement.value))
            if statement.qualifiers:
                for qualifier in statement.qualifiers:
                    columns.update(self.value_resolver.extract_columns(qualifier.value))
            if statement.references:
                for reference in statement.references:
                    columns.update(self.value_resolver.extract_columns(reference.value))

        if mapping_rule.statements:
            for statement in mapping_rule.statements:
                extend_with_statement(statement)

        return [col for col in columns if col]

    def _filter_dataframe(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
    ) -> pd.DataFrame:
        required_columns = self._required_columns(mapping_rule)
        selected_columns = [col for col in required_columns if col in dataframe.columns]
        filtered = dataframe[selected_columns].copy()

        str_cols = filtered.select_dtypes(include=["object", "string"]).columns
        for col in str_cols:
            filtered[col] = filtered[col].where(
                filtered[col].isna(), filtered[col].astype(str).str.strip()
            )

        # Critical columns: those needed to resolve label and unique key
        critical_columns: set[str] = set()
        critical_columns.update(self._extract_template_columns(mapping_rule.item.label))
        if (
            not critical_columns
            and mapping_rule.item.label
            and mapping_rule.item.label in filtered.columns
        ):
            critical_columns.add(mapping_rule.item.label)

        if mapping_rule.item.unique_key and mapping_rule.item.unique_key.value:
            uk_cols = self._extract_template_columns(mapping_rule.item.unique_key.value)
            if (
                not uk_cols
                and mapping_rule.item.unique_key.value in filtered.columns
            ):
                uk_cols.add(mapping_rule.item.unique_key.value)
            critical_columns.update(uk_cols)

        subset = [col for col in critical_columns if col in filtered.columns]
        if subset:
            filtered = filtered.dropna(subset=subset)

        return filtered.drop_duplicates()

    def _prepare_item_fields(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
    ) -> pd.DataFrame:
        """Add computed label/description/unique key helper columns."""
        df = dataframe.copy()
        df["__label"] = df.apply(
            lambda row: self._render_value(mapping_rule.item.label, row), axis=1
        )

        if mapping_rule.item.unique_key:
            df["__unique_key_value"] = df.apply(
                lambda row: self._render_value(mapping_rule.item.unique_key.value, row),
                axis=1,
            )
        else:
            df["__unique_key_value"] = None

        if mapping_rule.item.description:
            df["__description"] = df.apply(
                lambda row: self._render_value(mapping_rule.item.description, row),
                axis=1,
            )

        df["__label"] = df["__label"].apply(self._clean_value)
        df["__unique_key_value"] = df["__unique_key_value"].apply(self._clean_value)
        if "__description" in df.columns:
            df["__description"] = df["__description"].apply(self._clean_value)

        df = df.dropna(subset=["__label"])
        if mapping_rule.item.unique_key:
            df = df.dropna(subset=["__unique_key_value"])

        return df.drop_duplicates()

    def _collect_item_lookups(
        self,
        dataframe: pd.DataFrame,
        statements: list,
        context: MappingContext,
    ) -> tuple[set[str], dict[str, set[tuple[str, str]]]]:
        """Collect wikibase-item lookups needed for value resolution."""
        label_only: set[str] = set()
        unique_keys_by_property: dict[str, set[tuple[str, str]]] = {}

        def process_statement(statement):
            datatype = context.get_property_datatype(statement.property)
            if datatype == "wikibase-item":
                l_only, u_keys = self.value_resolver.extract_item_lookups(
                    statement.value, dataframe
                )
                label_only.update(l_only)
                for lbl, prop_label, val in u_keys:
                    if not prop_label or val is None:
                        continue
                    unique_keys_by_property.setdefault(prop_label, set()).add(
                        (lbl, val)
                    )
            if statement.qualifiers:
                for qualifier in statement.qualifiers:
                    process_statement(qualifier)
            if statement.references:
                for reference in statement.references:
                    process_statement(reference)

        for stmt in statements:
            process_statement(stmt)

        return label_only, unique_keys_by_property

    def _process_item_mapping(
        self,
        mapping: MappingRule,
        dataframe: pd.DataFrame,
        context: MappingContext,
    ) -> None:
        filtered_df = self._filter_dataframe(dataframe, mapping)
        prepared_df = self._prepare_item_fields(filtered_df, mapping)

        if prepared_df.empty:
            return

        # Prime property cache (ids + datatypes)
        context.ensure_properties(
            statements=mapping.statements,
            unique_keys=[mapping.item.unique_key] if mapping.item.unique_key else None,
        )

        # Prime wikibase-item value caches for statements
        if mapping.statements:
            label_only, unique_keys = self._collect_item_lookups(
                prepared_df,
                mapping.statements,
                context,
            )
            if label_only:
                context.ensure_qids_for_labels(label_only)
            for prop_label, keys in unique_keys.items():
                property_id, datatype = context.get_property_info(prop_label)
                if property_id:
                    context.ensure_qids_for_unique_keys(keys, property_id, datatype)

        uk = mapping.item.unique_key
        if not uk:
            raise ValueError("Unique key is required to locate existing items.")

        uk_property_id, uk_datatype = context.get_property_info(uk.property)
        if not uk_property_id:
            raise ValueError(f"Unique key property not found: {uk.property}")

        total_rows = len(prepared_df)
        for start in range(0, total_rows, self.chunk_size):
            chunk = prepared_df.iloc[start : start + self.chunk_size]
            if chunk.empty:
                continue

            keys = list(
                chunk[["__label", "__unique_key_value"]].itertuples(index=False, name=None)
            )

            context.ensure_qids_for_unique_keys(keys, uk_property_id, uk_datatype)

            with ItemBulkSearcher() as item_searcher:
                qids_found = item_searcher.find_qids_by_unique_key(
                    keys,
                    property_id=uk_property_id,
                    property_datatype=uk_datatype,
                    language=context.language,
                )

            if qids_found:
                found_records = [
                    {
                        "__label": label_value,
                        "__unique_key_value": unique_value,
                        "__qid": qid,
                    }
                    for (label_value, unique_value), qid in qids_found.items()
                ]
                found_df = pd.DataFrame(found_records)
                df_existing = chunk.merge(
                    found_df,
                    on=["__label", "__unique_key_value"],
                    how="inner",
                )
                df_new = (
                    chunk.merge(
                        found_df[["__label", "__unique_key_value"]],
                        on=["__label", "__unique_key_value"],
                        how="left",
                        indicator=True,
                    )
                    .loc[lambda x: x["_merge"] == "left_only"]
                    .drop(columns=["_merge"])
                )
            else:
                df_existing = chunk.iloc[0:0]
                df_new = chunk

            if not df_new.empty:
                self.creator.run(df_new, mapping, context)

            if self.updater and not df_existing.empty:
                self.updater.run(df_existing, mapping, context)

    def process(self, mapping_path: str) -> None:
        mapping_config = self._load_mapping_config(mapping_path)
        context = MappingContext(language=mapping_config.language)

        for csv_config in mapping_config.csv_files:
            dataframe = self._load_dataframe(csv_config, mapping_config)

            for mapping in csv_config.mappings:

                print(f"[V2] Processing item mapping: {mapping.item.label}")
                self.updater = UpdateStrategyFactory.for_mapping(
                    csv_config,
                    mapping,
                    self.claim_builder,
                )
                self._process_item_mapping(
                    mapping,
                    dataframe.copy(),
                    context,
                )
