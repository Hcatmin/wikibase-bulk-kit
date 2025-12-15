"""New mapping processor implementation using a composable pipeline."""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import time
import yaml
import pandas as pd

from wbk.mapping.models import (
    MappingRule,
    CSVFileConfig,
    MappingConfig,
    ItemSearchMode,
    UpdateAction,
)
from .pipeline import (
    MappingContext,
    ValueResolver,
    ClaimBuilder,
    UpdateStrategy,
    UpdateStrategyFactory,
    CreateItemsStep,
)

from RaiseWikibase.dbconnection import DBConnection


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

    def _resolve_actions(
        self,
        mapping_rule: MappingRule,
        csv_config: CSVFileConfig,
    ) -> tuple[bool, UpdateAction | None]:
        """Compute creation/update behavior for a mapping.

        Defaults:
        - No create/update params: create enabled.
        - Only update_action provided: create disabled.
        - Explicit create provided: respect it.
        """
        action = mapping_rule.update_action or csv_config.update_action
        create_flag = mapping_rule.create
        if create_flag is None:
            create_flag = csv_config.create
        if create_flag is None:
            create_flag = action is None
        return create_flag, action

    def _load_mapping_config(self, mapping_path: str) -> MappingConfig:
        mapping_file = Path(mapping_path)
        if not mapping_file.exists():
            raise FileNotFoundError(f"Mapping file not found: {mapping_path}")

        with open(mapping_file, "r", encoding="utf-8") as file_handle:
            mapping_data = yaml.safe_load(file_handle)

        return MappingConfig(**mapping_data)

    def _load_dataframe_chunks(
        self,
        csv_config: CSVFileConfig,
        mapping_config: MappingConfig,
    ) -> pd.io.parsers.TextFileReader:
        encoding = csv_config.encoding or mapping_config.encoding
        delimiter = csv_config.delimiter or mapping_config.delimiter
        decimal_separator = csv_config.decimal_separator or mapping_config.decimal_separator
        return pd.read_csv(
            csv_config.file_path,
            encoding=encoding,
            delimiter=delimiter,
            decimal=decimal_separator,
            chunksize=self.chunk_size,
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

    def _log_metrics(self, lines: list[str]) -> None:
        """Append metric lines to a log file with timestamps."""
        timestamp = datetime.utcnow().isoformat()
        log_path = Path("mapping_metrics.log")
        with open(log_path, "a", encoding="utf-8") as log_file:
            for line in lines:
                log_file.write(f"{timestamp} {line}\n")

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
        allow_duplicate_search_keys: bool,
    ) -> pd.DataFrame:
        """Add computed helper columns based on item search mode.

        Columns added depend on search mode:
        - LABEL: ["__label"]
        - LABEL_DESCRIPTION: ["__label", "__description"]
        - LABEL_SNAK: ["__label", "__snak_value"]

        Duplicate search keys are only allowed when explicitly enabled
        (used for update-only append-style mappings).
        """
        df = dataframe.copy()
        search_mode = mapping_rule.item.search_mode

        # Always add label
        df["__label"] = df.apply(
            lambda row: self._render_value(mapping_rule.item.label, row), axis=1
        )

        # Add search-mode-specific columns
        if search_mode == ItemSearchMode.LABEL_SNAK:
            df["__snak_value"] = df.apply(
                lambda row: self._render_value(
                    mapping_rule.item.snak.value, row
                ),
                axis=1,
            )
        elif search_mode == ItemSearchMode.LABEL_DESCRIPTION:
            df["__description"] = df.apply(
                lambda row: self._render_value(mapping_rule.item.description, row),
                axis=1,
            )

        if mapping_rule.label:
            df["__new_label"] = df.apply(
                lambda row: self._render_value(mapping_rule.label, row),
                axis=1,
            )
        if mapping_rule.description:
            df["__new_description"] = df.apply(
                lambda row: self._render_value(mapping_rule.description, row),
                axis=1,
            )

        # Drop rows with missing required search fields
        # Raise error if there are duplicated search columns (when required)
        if allow_duplicate_search_keys:
            return df

        match search_mode:
            case ItemSearchMode.LABEL:
                duplicated = df.duplicated(subset=["__label"])
                if duplicated.any():
                    raise ValueError(f"Duplicate labels found in data:\n {df[duplicated].head(10)}")
            case ItemSearchMode.LABEL_DESCRIPTION:
                duplicated = df.duplicated(subset=["__label", "__description"])
                if duplicated.any():
                    raise ValueError(f"Duplicate label+description pairs found in data:\n {df[duplicated].head(10)}")
            case ItemSearchMode.LABEL_SNAK:
                duplicated = df.duplicated(subset=["__label", "__snak_value"])          
                if duplicated.any():        
                    raise ValueError(f"Duplicate label+snak pairs found in data:\n {df[duplicated].head(10)}")

        return df

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
        csv_config: CSVFileConfig,
        mapping_config: MappingConfig,
        context: MappingContext,
        create_enabled: bool,
        update_action: UpdateAction | None,
    ) -> tuple[int, int]:
        if not create_enabled and not self.updater:
            return 0, 0

        allow_duplicates = (not create_enabled) and (
            update_action != UpdateAction.REPLACE_ALL
        )

        print(f"allow_duplicates: {allow_duplicates}")

        # Prime property cache (ids + datatypes)
        context.ensure_properties(mapping)

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

        seen_filtered_rows: set[tuple[object, ...]] = set()
        seen_search_keys: set[object] = set()
        created_count = 0
        updated_count = 0

        def normalize_value(value: object) -> object | None:
            try:
                if pd.isna(value):  # type: ignore[attr-defined]
                    return None
            except Exception:
                if value is None:
                    return None
            return value

        def build_row_key(row_values: tuple[object, ...]) -> tuple[object, ...]:
            return tuple(normalize_value(value) for value in row_values)

        def drop_seen_filtered_rows(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return df

            mask: list[bool] = []
            new_keys: list[tuple[object, ...]] = []
            for row_values in df.itertuples(index=False, name=None):
                key = build_row_key(row_values)
                if key in seen_filtered_rows:
                    mask.append(False)
                else:
                    mask.append(True)
                    new_keys.append(key)

            if mask and not all(mask):
                df = df.loc[mask]
            seen_filtered_rows.update(new_keys)
            return df

        def ensure_unique_search_keys(df: pd.DataFrame) -> None:
            if allow_duplicates or df.empty:
                return

            duplicate_indices: list[int] = []
            new_keys: list[object] = []
            error_message = ""

            match search_mode:
                case ItemSearchMode.LABEL:
                    labels = df["__label"].map(normalize_value)
                    for idx, label in zip(labels.index, labels):
                        if label in seen_search_keys:
                            duplicate_indices.append(idx)
                        else:
                            new_keys.append(label)
                    error_message = "Duplicate labels found in data:"
                case ItemSearchMode.LABEL_DESCRIPTION:
                    labels = df["__label"].map(normalize_value)
                    descriptions = df["__description"].map(normalize_value)
                    for idx, label, desc in zip(labels.index, labels, descriptions):
                        key = (label, desc)
                        if key in seen_search_keys:
                            duplicate_indices.append(idx)
                        else:
                            new_keys.append(key)
                    error_message = "Duplicate label+description pairs found in data:"
                case ItemSearchMode.LABEL_SNAK:
                    labels = df["__label"].map(normalize_value)
                    snak_values = df["__snak_value"].map(normalize_value)
                    for idx, label, snak_value in zip(labels.index, labels, snak_values):
                        key = (label, snak_value)
                        if key in seen_search_keys:
                            duplicate_indices.append(idx)
                        else:
                            new_keys.append(key)
                    error_message = "Duplicate label+snak pairs found in data:"
                case _:
                    raise ValueError(f"Unknown search mode: {search_mode}")

            if duplicate_indices:
                duplicate_sample = df.loc[duplicate_indices].head(10)
                raise ValueError(f"{error_message}\n {duplicate_sample}")

            seen_search_keys.update(new_keys)

        reader = self._load_dataframe_chunks(csv_config, mapping_config)
        try:
            for dataframe in reader:
                filtered_df = self._filter_dataframe(dataframe, mapping)
                filtered_df = drop_seen_filtered_rows(filtered_df)

                if filtered_df.empty:
                    continue

                prepared_df = self._prepare_item_fields(
                    filtered_df,
                    mapping,
                    allow_duplicates,
                )

                if prepared_df.empty:
                    continue

                ensure_unique_search_keys(prepared_df)

                # Prime wikibase-item value caches for statements per chunk
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
                            context.ensure_qids_for_unique_keys(
                                keys,
                                property_id,
                                datatype,
                                allow_ambiguous=allow_duplicates,
                            )

                working_df = prepared_df
                while not working_df.empty:
                    chunk_size = min(self.chunk_size, len(working_df))
                    chunk = working_df.iloc[:chunk_size].copy()
                    working_df = working_df.iloc[chunk_size:].reset_index(drop=True)

                    if chunk.empty:
                        continue

                    # Process chunk based on search mode
                    found_df, merge_columns = self._search_items_in_chunk(
                        chunk=chunk,
                        search_mode=search_mode,
                        context=context,
                        snak_property_id=snak_property_id,
                        snak_datatype=snak_datatype,
                        allow_ambiguous=allow_duplicates,
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

                    if create_enabled and not df_new.empty:
                        self.creator.run(df_new, mapping, context)
                        created_count += len(df_new)
                        del df_new
                    elif not create_enabled and not df_new.empty:
                        print(
                            f"[V2] Skipping creation for {len(df_new)} rows "
                            f"(create disabled for mapping: {mapping.item.label})"
                        )
                        del df_new

                    if self.updater and not df_existing.empty:
                        self.updater.run(df_existing, mapping, context)
                        if "__qid" in df_existing.columns:
                            updated_count += df_existing["__qid"].nunique()
                        else:
                            updated_count += len(df_existing)
                        del df_existing
        finally:
            reader.close()
        return created_count, updated_count

    def _search_items_in_chunk(
        self,
        chunk: pd.DataFrame,
        search_mode: ItemSearchMode,
        context: MappingContext,
        snak_property_id: str | None = None,
        snak_datatype: str | None = None,
        allow_ambiguous: bool = False,
    ) -> tuple[pd.DataFrame, list[str]]:
        """Search for existing items based on the search mode.

        Returns:
            Tuple of (found_df with __qid column, list of merge columns).
        """
        if search_mode == ItemSearchMode.LABEL:
            return self._search_by_label(chunk, context, allow_ambiguous)
        elif search_mode == ItemSearchMode.LABEL_DESCRIPTION:
            return self._search_by_label_and_description(chunk, context)
        elif search_mode == ItemSearchMode.LABEL_SNAK:
            return self._search_by_label_and_snak(
                chunk, context, snak_property_id, snak_datatype, allow_ambiguous
            )
        else:
            raise ValueError(f"Unknown search mode: {search_mode}")

    def _search_by_label(
        self,
        chunk: pd.DataFrame,
        context: MappingContext,
        allow_ambiguous: bool = False,
    ) -> tuple[pd.DataFrame, list[str]]:
        """Search items by label only."""
        merge_columns = ["__label"]

        labels = chunk["__label"].dropna().unique().tolist()
        context.ensure_qids_for_labels(labels)

        items_found = context.item_searcher.find_items_by_labels(
            labels,
            language=context.language,
            allow_ambiguous=allow_ambiguous,
        )

        found_records = []
        for label, item in items_found.items():
            if item:
                context.cache_item(item)
                found_records.append({"__label": label, "__qid": item.get("id")})

        if found_records:
            found_df = pd.DataFrame(found_records)
        else:
            found_df = pd.DataFrame(columns=["__label", "__qid"])

        return found_df, merge_columns

    def _search_by_label_and_description(
        self,
        chunk: pd.DataFrame,
        context: MappingContext,
    ) -> tuple[pd.DataFrame, list[str]]:
        """Search items by label and description."""
        merge_columns = ["__label", "__description"]

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
                context.cache_item(item)
                found_records.append({
                    "__label": label,
                    "__description": desc,
                    "__qid": item.get("id"),
                })

        if found_records:
            found_df = pd.DataFrame(found_records)
        else:
            found_df = pd.DataFrame(columns=["__label", "__description", "__qid"])

        return found_df, merge_columns

    def _search_by_label_and_snak(
        self,
        chunk: pd.DataFrame,
        context: MappingContext,
        property_id: str | None,
        datatype: str | None,
        allow_ambiguous: bool = False,
    ) -> tuple[pd.DataFrame, list[str]]:
        """Search items by label and property-value (snak)."""
        merge_columns = ["__label", "__snak_value"]

        keys = list(
            chunk[merge_columns].dropna().itertuples(index=False, name=None)
        )
        context.ensure_qids_for_snaks(
            keys,
            property_id,
            datatype,
            allow_ambiguous=allow_ambiguous,
        )

        items_found = context.item_searcher.find_items_by_label_and_snak(
            keys,
            property_id=property_id,
            property_datatype=datatype,
            language=context.language,
            allow_ambiguous=allow_ambiguous,
        )

        found_records = []
        for (label, snak_value), item in items_found.items():
            if item:
                context.cache_item(item)
                found_records.append({
                    "__label": label,
                    "__snak_value": snak_value,
                    "__qid": item.get("id"),
                })

        if found_records:
            found_df = pd.DataFrame(found_records)
        else:
            found_df = pd.DataFrame(columns=["__label", "__snak_value", "__qid"])

        return found_df, merge_columns

    def process(self, mapping_path: str) -> None:
        mapping_config = self._load_mapping_config(mapping_path)
        if mapping_config.chunk_size:
            self.chunk_size = mapping_config.chunk_size
        context = MappingContext(language=mapping_config.language)

        start_time = time.perf_counter()
        db_connection = DBConnection()
        start_items = db_connection.get_last_eid("wikibase-item")
        db_connection.conn.close()
        total_created = 0
        total_updated = 0

        for csv_config in mapping_config.csv_files:
            for mapping in csv_config.mappings:

                print(f"[V2] Processing item mapping: {mapping.item.label}")
                create_enabled, resolved_action = self._resolve_actions(
                    mapping,
                    csv_config,
                )
                self.updater = UpdateStrategyFactory.for_mapping(
                    csv_config,
                    mapping,
                    self.claim_builder,
                    action=resolved_action,
                )
                created, updated = self._process_item_mapping(
                    mapping,
                    csv_config,
                    mapping_config,
                    context,
                    create_enabled,
                    resolved_action,
                )
                total_created += created
                total_updated += updated

        end_time = time.perf_counter()
        
        db_connection = DBConnection()
        end_items = db_connection.get_last_eid("wikibase-item")
        db_connection.conn.close()
        created_delta = None
        if start_items is not None and end_items is not None:
            created_delta = end_items - start_items

        elapsed = end_time - start_time
        metric_lines = [
            f"[Metrics] Runtime: {elapsed:.2f}s | "
            f"created (rows): {total_created} | "
            f"updated (items): {total_updated}"
        ]
        if created_delta is not None:
            metric_lines.append(
                f"[Metrics] wikibase-item id delta: {created_delta} "
                f"(start={start_items}, end={end_items})"
            )
        self._log_metrics(metric_lines)
