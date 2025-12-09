"""New mapping processor implementation using a composable pipeline."""

from __future__ import annotations

from pathlib import Path
import yaml
import pandas as pd

from wbk.mapping.models import (
    MappingRule,
    CSVFileConfig,
    MappingConfig,
    ItemSearchMode,
)
from .pipeline import (
    MappingContext,
    ValueResolver,
    ClaimBuilder,
    UpdateStrategy,
    UpdateStrategyFactory,
    CreateItemsStep,
)


class MappingProcessor:
    """Processes mapping configurations using the new pipeline architecture."""

    def __init__(self) -> None:
        self._chunk_size = 1000
        self.value_resolver = ValueResolver()
        self.claim_builder = ClaimBuilder(self.value_resolver)
        self.creator = CreateItemsStep(
            claim_builder=self.claim_builder,
        )
        self.updater: UpdateStrategy | None = None

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @chunk_size.setter
    def chunk_size(self, value: int) -> None:
        self._chunk_size = value

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
        """Collect dataframe columns needed to resolve item and snak values."""
        columns: set[str] = set()
        item_def = mapping_rule.item

        # Item label templates
        columns.update(self._extract_template_columns(item_def.label))
        if "{" not in item_def.label and item_def.label:
            columns.add(item_def.label)

        # SnakMatcher templates
        if item_def.snak and item_def.snak.value:
            columns.update(self._extract_template_columns(item_def.snak.value))
            if "{" not in item_def.snak.value:
                columns.add(item_def.snak.value)

        # Description templates
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

        # Critical columns: those needed to resolve label and search key
        critical_columns: set[str] = set()
        critical_columns.update(self._extract_template_columns(mapping_rule.item.label))
        if (
            not critical_columns
            and mapping_rule.item.label
            and mapping_rule.item.label in filtered.columns
        ):
            critical_columns.add(mapping_rule.item.label)

        # SnakMatcher columns (for snak-based search)
        if mapping_rule.item.snak and mapping_rule.item.snak.value:
            stmt_cols = self._extract_template_columns(
                mapping_rule.item.snak.value
            )
            if (
                not stmt_cols
                and mapping_rule.item.snak.value in filtered.columns
            ):
                stmt_cols.add(mapping_rule.item.snak.value)
            critical_columns.update(stmt_cols)

        # Description columns (for description-based search)
        if mapping_rule.item.description:
            desc_cols = self._extract_template_columns(mapping_rule.item.description)
            if (
                not desc_cols
                and mapping_rule.item.description in filtered.columns
            ):
                desc_cols.add(mapping_rule.item.description)
            critical_columns.update(desc_cols)

        subset = [col for col in critical_columns if col in filtered.columns]
        if subset:
            filtered = filtered.dropna(subset=subset)

        return filtered.drop_duplicates()

    def _prepare_item_fields(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
    ) -> pd.DataFrame:
        """Add computed helper columns based on item search mode.

        Columns added depend on search mode:
        - LABEL: ["__label"]
        - LABEL_DESCRIPTION: ["__label", "__description"]
        - LABEL_SNAK: ["__label", "__snak_value"]
        """
        df = dataframe.copy()
        search_mode = mapping_rule.item.search_mode

        # Always add label
        df["__label"] = df.apply(
            lambda row: self._render_value(mapping_rule.item.label, row), axis=1
        )
        df["__label"] = df["__label"].apply(self._clean_value)

        # Add search-mode-specific columns
        if search_mode == ItemSearchMode.LABEL_SNAK:
            df["__snak_value"] = df.apply(
                lambda row: self._render_value(
                    mapping_rule.item.snak.value, row
                ),
                axis=1,
            )
            df["__snak_value"] = df["__snak_value"].apply(self._clean_value)
        elif search_mode == ItemSearchMode.LABEL_DESCRIPTION:
            df["__description"] = df.apply(
                lambda row: self._render_value(mapping_rule.item.description, row),
                axis=1,
            )
            df["__description"] = df["__description"].apply(self._clean_value)

        # Drop rows with missing required search fields
        df = df.dropna(subset=["__label"])
        if search_mode == ItemSearchMode.LABEL_SNAK:
            df = df.dropna(subset=["__snak_value"])
        elif search_mode == ItemSearchMode.LABEL_DESCRIPTION:
            df = df.dropna(subset=["__description"])

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
        context.ensure_properties(mapping)

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

        search_mode = mapping.item.search_mode

        # Resolve property info for snak-based search
        snak_property_id: str | None = None
        snak_datatype: str | None = None
        if search_mode == ItemSearchMode.LABEL_SNAK:
            stmt = mapping.item.snak
            if not stmt:
                raise ValueError(
                    "Statement matcher is required for LABEL_SNAK search mode."
                )
            snak_property_id, snak_datatype = context.get_property_info(stmt.property)
            if not snak_property_id:
                raise ValueError(f"Statement property not found: {stmt.property}")

        while not prepared_df.empty:
            # Pop chunk from the beginning to free memory
            chunk_size = min(self.chunk_size, len(prepared_df))
            chunk = prepared_df.iloc[:chunk_size].copy()
            prepared_df = prepared_df.iloc[chunk_size:].reset_index(drop=True)

            if chunk.empty:
                continue

            # Process chunk based on search mode
            found_df, merge_columns = self._search_items_in_chunk(
                chunk=chunk,
                search_mode=search_mode,
                context=context,
                snak_property_id=snak_property_id,
                snak_datatype=snak_datatype,
            )

            df_existing = chunk.merge(
                found_df,
                on=merge_columns,
                how="inner",
            )
            df_new = (
                chunk.merge(
                    found_df[merge_columns],
                    on=merge_columns,
                    how="left",
                    indicator=True,
                )
                .loc[lambda x: x["_merge"] == "left_only"]
                .drop(columns=["_merge"])
            )
            del found_df
            del chunk

            if not df_new.empty:
                self.creator.run(df_new, mapping, context)
                del df_new

            if self.updater and not df_existing.empty:
                self.updater.run(df_existing, mapping, context)
                del df_existing

    def _search_items_in_chunk(
        self,
        chunk: pd.DataFrame,
        search_mode: ItemSearchMode,
        context: MappingContext,
        snak_property_id: str | None = None,
        snak_datatype: str | None = None,
    ) -> tuple[pd.DataFrame, list[str]]:
        """Search for existing items based on the search mode.

        Returns:
            Tuple of (found_df with __item column, list of merge columns).
        """
        if search_mode == ItemSearchMode.LABEL:
            return self._search_by_label(chunk, context)
        elif search_mode == ItemSearchMode.LABEL_DESCRIPTION:
            return self._search_by_label_and_description(chunk, context)
        elif search_mode == ItemSearchMode.LABEL_SNAK:
            return self._search_by_label_and_snak(
                chunk, context, snak_property_id, snak_datatype
            )
        else:
            raise ValueError(f"Unknown search mode: {search_mode}")

    def _search_by_label(
        self,
        chunk: pd.DataFrame,
        context: MappingContext,
    ) -> tuple[pd.DataFrame, list[str]]:
        """Search items by label only."""
        merge_columns = ["__label"]

        # Check for duplicates
        if chunk.duplicated(subset=merge_columns).any():
            duplicated = chunk[chunk.duplicated(subset=merge_columns, keep=False)]
            raise ValueError(
                f"Duplicate labels found in chunk. Use description or statement "
                f"to disambiguate: {duplicated['__label'].unique().tolist()}"
            )

        labels = chunk["__label"].dropna().unique().tolist()
        context.ensure_qids_for_labels(labels)

        items_found = context.item_searcher.find_items_by_labels(
            labels, language=context.language
        )

        found_records = []
        for label, item in items_found.items():
            if item:
                found_records.append({"__label": label, "__item": item})

        if found_records:
            found_df = pd.DataFrame(found_records)
        else:
            found_df = pd.DataFrame(columns=["__label", "__item"])

        return found_df, merge_columns

    def _search_by_label_and_description(
        self,
        chunk: pd.DataFrame,
        context: MappingContext,
    ) -> tuple[pd.DataFrame, list[str]]:
        """Search items by label and description."""
        merge_columns = ["__label", "__description"]

        # Check for duplicates
        if chunk.duplicated(subset=merge_columns).any():
            duplicated = chunk[chunk.duplicated(subset=merge_columns, keep=False)]
            raise ValueError(
                f"Duplicate label+description pairs found in chunk: "
                f"{duplicated[merge_columns].drop_duplicates().values.tolist()}"
            )

        pairs = list(
            chunk[merge_columns].dropna().itertuples(index=False, name=None)
        )
        context.ensure_qids_for_labels_and_descriptions(pairs)

        items_found = context.item_searcher.find_items_by_labels_and_descriptions(
            pairs, language=context.language
        )

        found_records = []
        for (label, desc), item in items_found.items():
            if item:
                found_records.append({
                    "__label": label,
                    "__description": desc,
                    "__item": item,
                })

        if found_records:
            found_df = pd.DataFrame(found_records)
        else:
            found_df = pd.DataFrame(columns=["__label", "__description", "__item"])

        return found_df, merge_columns

    def _search_by_label_and_snak(
        self,
        chunk: pd.DataFrame,
        context: MappingContext,
        property_id: str | None,
        datatype: str | None,
    ) -> tuple[pd.DataFrame, list[str]]:
        """Search items by label and property-value (snak)."""
        merge_columns = ["__label", "__snak_value"]

        # Check for duplicates
        if chunk.duplicated(subset=merge_columns).any():
            duplicated = chunk[chunk.duplicated(subset=merge_columns, keep=False)]
            raise ValueError(
                f"Duplicate label+snak pairs found in chunk: "
                f"{duplicated[merge_columns].drop_duplicates().values.tolist()}"
            )

        keys = list(
            chunk[merge_columns].dropna().itertuples(index=False, name=None)
        )
        context.ensure_qids_for_snaks(keys, property_id, datatype)

        items_found = context.item_searcher.find_items_by_label_and_snak(
            keys,
            property_id=property_id,
            property_datatype=datatype,
            language=context.language,
        )

        found_records = []
        for (label, snak_value), item in items_found.items():
            if item:
                found_records.append({
                    "__label": label,
                    "__snak_value": snak_value,
                    "__item": item,
                })

        if found_records:
            found_df = pd.DataFrame(found_records)
        else:
            found_df = pd.DataFrame(columns=["__label", "__snak_value", "__item"])

        return found_df, merge_columns

    def process(self, mapping_path: str) -> None:
        mapping_config = self._load_mapping_config(mapping_path)
        if mapping_config.chunk_size:
            self.chunk_size = mapping_config.chunk_size
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
