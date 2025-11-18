"""New mapping processor implementation using a composable pipeline."""

from __future__ import annotations

from pathlib import Path
import re
import yaml
import pandas as pd

from ..config.manager import ConfigManager
from .models import (
    MappingConfig,
    CSVFileConfig,
    ItemMapping,
    StatementMapping,
    ClaimMapping,
)
from .pipeline import (
    MappingContext,
    ValueResolver,
    ClaimBuilder,
    UpdateStrategyFactory,
    CreateItemsStep,
)

import wbk.mapping.utils as utils


class MappingProcessor:
    """Processes mapping configurations using the new pipeline architecture."""

    def __init__(self, config_manager: ConfigManager, chunk_size: int = 1000) -> None:
        self.config_manager = config_manager
        self.chunk_size = chunk_size
        self.value_resolver = ValueResolver()
        self.claim_builder = ClaimBuilder(self.value_resolver)

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

    def _filter_dataframe(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
    ) -> pd.DataFrame:
        columns = {item_mapping.label_column}

        if item_mapping.description:
            template_columns = re.findall(r"\{(.*?)\}", item_mapping.description)
            if template_columns:
                columns.update(template_columns)
            elif item_mapping.description in dataframe.columns:
                columns.add(item_mapping.description)

        def extend_with_statement(statement: StatementMapping):
            columns.update(self.value_resolver.extract_columns(statement.value))
            if statement.qualifiers:
                for qualifier in statement.qualifiers:
                    columns.update(self.value_resolver.extract_columns(qualifier.value))
            if statement.references:
                for reference in statement.references:
                    columns.update(self.value_resolver.extract_columns(reference.value))

        if item_mapping.statements:
            for statement in item_mapping.statements:
                extend_with_statement(statement)

        selected_columns = [col for col in columns if col in dataframe.columns]
        filtered = dataframe[selected_columns]
        filtered[item_mapping.label_column] = (
            filtered[item_mapping.label_column].astype(str).str.strip()
        )
        # drop rows with NaN in any of the selected columns
        filtered = filtered.drop_duplicates().dropna(subset=selected_columns)
        return filtered

    def _collect_wikibase_value_keys(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
    ) -> list[tuple[str, str | None]]:
        """Collect unique (label, description) keys used in wikibase-item values."""
        if not item_mapping.statements:
            return []

        keys: set[tuple[str, str | None]] = set()

        def extend_from_mapping(mapping: StatementMapping | ClaimMapping | None):
            if not mapping or mapping.datatype != "wikibase-item":
                return
            extracted = self.value_resolver.extract_label_description_pairs(
                mapping.value,
                dataframe,
            )
            keys.update(extracted)

        for statement in item_mapping.statements:
            extend_from_mapping(statement)
            if statement.qualifiers:
                for qualifier in statement.qualifiers:
                    extend_from_mapping(qualifier)
            if statement.references:
                for reference in statement.references:
                    extend_from_mapping(reference)

        return list(keys)

    def _process_item_mapping(
        self,
        csv_config: CSVFileConfig,
        item_mapping: ItemMapping,
        dataframe: pd.DataFrame,
        context: MappingContext,
    ) -> None:
        filtered_df = self._filter_dataframe(dataframe, item_mapping)
        value_keys = self._collect_wikibase_value_keys(filtered_df, item_mapping)

        context.ensure_property_ids(item_mapping.statements)
        context.ensure_qids(value_keys)

        creator = CreateItemsStep(
            claim_builder=self.claim_builder,
            chunk_size=self.chunk_size,
        )
        strategy = UpdateStrategyFactory.for_mapping(
            csv_config,
            item_mapping,
            claim_builder=self.claim_builder,
        )
        if strategy:
            strategy.chunk_size = self.chunk_size

        if filtered_df.empty:
            return

        label_column = item_mapping.label_column
        total_rows = len(filtered_df)
        for start in range(0, total_rows, self.chunk_size):
            chunk = filtered_df.iloc[start : start + self.chunk_size]
            if chunk.empty:
                continue

            chunk['description'] = utils.create_description(chunk, item_mapping.description)

            items_found = context.item_searcher.find_qids(chunk[[label_column, 'description']].itertuples(index=False, name=None))

            items_found = {k:v for k,v in items_found.items() if v is not None}

            items_found_keys = pd.DataFrame(items_found.keys(), columns=[label_column, 'description'])

            df_existing = pd.merge(
                chunk,
                items_found_keys,
                on=[label_column, 'description'],
                how='inner',
            )
            df_new = pd.merge(
                chunk,
                items_found_keys,
                on=[label_column, 'description'],
                how='outer',
                indicator=True,
            ).loc[lambda x: x['_merge'] == 'left_only'].drop(columns=['_merge'])

            if not df_new.empty:
                creator.run(df_new, item_mapping, context)

            if strategy and not df_existing.empty:
                strategy.run(df_existing, item_mapping, context)

    def process(self, mapping_path: str) -> None:
        mapping_config = self._load_mapping_config(mapping_path)
        context = MappingContext(language=mapping_config.language)

        for csv_config in mapping_config.csv_files:
            dataframe = self._load_dataframe(csv_config, mapping_config)
            if not csv_config.item_mapping:
                continue
            for item_mapping in csv_config.item_mapping:
                print(f"[V2] Processing item mapping: {item_mapping.label_column}")
                self._process_item_mapping(
                    csv_config,
                    item_mapping,
                    dataframe.copy(),
                    context,
                )
