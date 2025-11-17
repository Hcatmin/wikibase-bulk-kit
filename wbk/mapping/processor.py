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
)
from .pipeline import (
    MappingContext,
    ValueResolver,
    ClaimBuilder,
    UpdateStrategyFactory,
    CreateItemsStep,
)


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
        filtered = dataframe[selected_columns].dropna(
            subset=[item_mapping.label_column]
        )
        filtered[item_mapping.label_column] = (
            filtered[item_mapping.label_column].astype(str).str.strip()
        )
        filtered = filtered.drop_duplicates()
        return filtered

    def _collect_labels(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
    ) -> list[str]:
        labels: list[str] = []
        label_column = item_mapping.label_column
        if label_column in dataframe.columns:
            labels.extend(
                dataframe[label_column].drop_duplicates().dropna().tolist()
            )

        if item_mapping.statements:
            for statement in item_mapping.statements:
                labels.extend(
                    self.value_resolver.extract_labels(
                        statement.value,
                        statement.datatype,
                        dataframe,
                    )
                )

                if statement.qualifiers:
                    for qualifier in statement.qualifiers:
                        labels.extend(
                            self.value_resolver.extract_labels(
                                qualifier.value,
                                qualifier.datatype,
                                dataframe,
                            )
                        )

                if statement.references:
                    for reference in statement.references:
                        labels.extend(
                            self.value_resolver.extract_labels(
                                reference.value,
                                reference.datatype,
                                dataframe,
                            )
                        )

        return labels

    def _process_item_mapping(
        self,
        csv_config: CSVFileConfig,
        item_mapping: ItemMapping,
        dataframe: pd.DataFrame,
        context: MappingContext,
    ) -> None:
        filtered_df = self._filter_dataframe(dataframe, item_mapping)
        labels = self._collect_labels(filtered_df, item_mapping)

        context.ensure_property_ids(item_mapping.statements)
        context.ensure_qids(labels)

        label_column = item_mapping.label_column
        known_labels = set(context.qid_cache.keys())
        existing_mask = filtered_df[label_column].isin(known_labels)
        df_existing = filtered_df[existing_mask]
        df_new = filtered_df[~existing_mask]

        creator = CreateItemsStep(
            claim_builder=self.claim_builder,
            chunk_size=self.chunk_size,
        )
        creator.run(df_new, item_mapping, context)

        strategy = UpdateStrategyFactory.for_mapping(
            csv_config,
            item_mapping,
            claim_builder=self.claim_builder,
        )
        if strategy:
            strategy.chunk_size = self.chunk_size
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
