"""Update strategies and creation helpers for the mapping pipeline."""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from typing import Iterable, Type, Any, NamedTuple

import pandas as pd

from RaiseWikibase.datamodel import entity, label, description
from RaiseWikibase.raiser import batch

from ..models import ItemMapping, UpdateAction, StatementMapping, CSVFileConfig
from .context import MappingContext
from .claim_builder import ClaimBuilder
from wbk.processor.bulk_item_search import ItemBulkSearcher
import re


class BatchMixin:
    """Utility mixin for batching operations."""

    def _flush_items(self, items: list[dict], new: bool) -> None:
        if not items:
            return
        batch("wikibase-item", items, new=new)
        items.clear()

    def _get_columns(
        self,
        from_template: str,
        dataframe_columns: Iterable[str],
    ) -> list[str]:
        list_columns = re.findall(r"\{(.*?)\}", from_template)
        assert set(list_columns) <= set(dataframe_columns)  # template columns must be in dataframe
        return list_columns

    def _build_description_value(
        self,
        item_mapping: ItemMapping,
        row: pd.Series,
        template_columns: list[str],
        description_from_column: bool,
    ) -> str | None:
        if not item_mapping.description:
            return None
        if template_columns:
            description_template = item_mapping.description
            for col in template_columns:
                description_template = description_template.replace(
                    "{" + col + "}",
                    str(row[col]),
                )
            return description_template
        if description_from_column:
            return str(row[item_mapping.description])
        return item_mapping.description

    @staticmethod
    def _normalize_description_key(description_value: str | None) -> str | None:
        if description_value is None:
            return None
        normalized = str(description_value).strip()
        return normalized or None


class CreateItemsStep(BatchMixin):
    """Handles creation of new items in chunks."""

    def __init__(
        self,
        claim_builder: ClaimBuilder | None = None,
        chunk_size: int = 1000,
    ) -> None:
        self.claim_builder = claim_builder or ClaimBuilder()
        self.chunk_size = chunk_size

    def run(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
        context: MappingContext,
    ) -> None:
        if dataframe.empty:
            return

        items: list[dict] = []

        list_columns = self._get_columns(
            item_mapping.description,
            dataframe.columns
        ) if item_mapping.description else []
        for _, row in dataframe.iterrows():
            item_name = str(row[item_mapping.label_column])
            labels = label(context.language, item_name)

            if item_mapping.description:
                description_template = item_mapping.description
                for col in list_columns:
                    description_template = description_template.replace(
                        "{" + col + "}",
                        str(row[col])
                    )
                descriptions = description(
                    context.language,
                    description_template
                )
            else:
                descriptions = {}

            item = entity(
                labels=labels,
                aliases={},
                descriptions=descriptions,
                claims={},
                etype="item",
            )

            self.claim_builder.apply_statements(
                item,
                row,
                item_mapping.statements,
                context,
            )
            items.append(item)

        self._flush_items(items, new=True)
        context.verify_items_created()


class UpdateStrategy(BatchMixin, ABC):
    """Base class for update strategies."""

    class RowContext(NamedTuple):
        row: pd.Series
        label: str
        description_value: str | None
        normalized_description: str | None

    def __init__(
        self,
        claim_builder: ClaimBuilder | None = None,
        chunk_size: int = 1000,
    ) -> None:
        self.claim_builder = claim_builder or ClaimBuilder()
        self.chunk_size = chunk_size

    @abstractmethod
    def run(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
        context: MappingContext,
    ) -> None:
        raise NotImplementedError

    def _prepare_row_contexts(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
    ) -> list["UpdateStrategy.RowContext"]:
        if dataframe.empty:
            return []

        list_columns = (
            self._get_columns(item_mapping.description, dataframe.columns)
            if item_mapping.description
            else []
        )
        description_from_column = (
            item_mapping.description
            and not list_columns
            and item_mapping.description in dataframe.columns
        )

        contexts: list[UpdateStrategy.RowContext] = []
        for _, row in dataframe.iterrows():
            item_label = str(row[item_mapping.label_column])
            description_value = self._build_description_value(
                item_mapping,
                row,
                list_columns,
                bool(description_from_column),
            )
            normalized_description = self._normalize_description_key(description_value)
            contexts.append(
                UpdateStrategy.RowContext(
                    row=row,
                    label=item_label,
                    description_value=description_value,
                    normalized_description=normalized_description,
                )
            )
        return contexts

    @staticmethod
    def _collect_lookup_keys(
        contexts: list["UpdateStrategy.RowContext"],
    ) -> list[tuple[str, str | None]]:
        keys: list[tuple[str, str | None]] = []
        seen: set[tuple[str, str | None]] = set()
        for context in contexts:
            key = (context.label, context.normalized_description)
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
        return keys


class ReplaceAllStrategy(UpdateStrategy):
    """Replace the full set of claims for existing items."""

    def run(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
        context: MappingContext,
    ) -> None:
        if dataframe.empty:
            return

        contexts = self._prepare_row_contexts(dataframe, item_mapping)
        if not contexts:
            return

        keys = self._collect_lookup_keys(contexts)
        with ItemBulkSearcher() as item_searcher:
            qids = item_searcher.find_qids(keys)

        items: list[dict] = []
        for row_ctx in contexts:
            key = (row_ctx.label, row_ctx.normalized_description)
            qid = qids.get(key)
            if not qid and row_ctx.normalized_description:
                qid = qids.get((row_ctx.label, None))
            if not qid:
                continue

            labels = label(context.language, row_ctx.label)
            if row_ctx.description_value:
                descriptions = description(context.language, row_ctx.description_value)
            else:
                descriptions = {}

            item = entity(
                labels=labels,
                aliases={},
                descriptions=descriptions,
                claims={},
                etype="item",
            )
            item["id"] = qid

            self.claim_builder.apply_statements(
                item, row_ctx.row, item_mapping.statements, context
            )
            items.append(item)

        self._flush_items(items, new=False)


class AppendOrReplaceStrategy(UpdateStrategy):
    """Append or replace claims depending on existing values."""

    def run(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
        context: MappingContext,
    ) -> None:
        if dataframe.empty:
            return

        from RaiseWikibase.utils import is_same_claim  # local import to avoid cycles

        contexts = self._prepare_row_contexts(dataframe, item_mapping)
        if not contexts:
            return

        keys = self._collect_lookup_keys(contexts)
        
        with ItemBulkSearcher() as item_searcher:
            items_by_key = item_searcher.find_items(
                keys,
                language=context.language,
            )

        items: list[dict] = []
        for row_ctx in contexts:
            key = (row_ctx.label, row_ctx.normalized_description)
            existing_item = items_by_key.get(key) or items_by_key.get((row_ctx.label, None))
            if not existing_item:
                continue

            existing_item = copy.deepcopy(existing_item)
            existing_item["labels"] = label(context.language, row_ctx.label)

            if row_ctx.description_value:
                existing_item["descriptions"] = description(
                    context.language, row_ctx.description_value
                )

            if item_mapping.statements:
                for statement in item_mapping.statements:
                    new_claim_dict = self.claim_builder.build_claim(
                        statement, row_ctx.row, context
                    )
                    if not new_claim_dict:
                        continue

                    property_id = context.get_property_id(
                        statement.property_id, statement.property_label
                    )
                    new_claim = new_claim_dict[property_id][0]

                    claims = existing_item.setdefault("claims", {})
                    if property_id in claims:
                        existing_claims = claims[property_id]
                        claim_found = False
                        for idx, existing_claim in enumerate(
                            existing_claims
                        ):
                            if is_same_claim(new_claim, existing_claim):
                                if existing_claim.get("id"):
                                    new_claim["id"] = existing_claim["id"]
                                existing_claims[idx] = new_claim
                                claim_found = True
                                break

                        if not claim_found:
                            existing_claims.append(new_claim)
                    else:
                        claims[property_id] = [new_claim]

            items.append(existing_item)

        self._flush_items(items, new=False)


class ForceAppendStrategy(UpdateStrategy):
    """Always append new claims even if duplicates exist."""

    def run(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
        context: MappingContext,
    ) -> None:
        if dataframe.empty:
            return

        contexts = self._prepare_row_contexts(dataframe, item_mapping)
        if not contexts:
            return

        keys = self._collect_lookup_keys(contexts)
        with ItemBulkSearcher() as item_searcher:
            items_by_key = item_searcher.find_items(
                keys,
                language=context.language,
            )

        items: list[dict] = []
        for row_ctx in contexts:
            key = (row_ctx.label, row_ctx.normalized_description)
            existing_item = items_by_key.get(key) or items_by_key.get((row_ctx.label, None))
            if not existing_item:
                continue

            existing_item = copy.deepcopy(existing_item)
            existing_item["labels"] = label(context.language, row_ctx.label)

            if row_ctx.description_value:
                existing_item["descriptions"] = description(
                    context.language, row_ctx.description_value
                )

            if item_mapping.statements:
                for statement in item_mapping.statements:
                    new_claim_dict = self.claim_builder.build_claim(
                        statement, row_ctx.row, context
                    )
                    if not new_claim_dict:
                        continue

                    property_id = context.get_property_id(
                        statement.property_id, statement.property_label
                    )
                    new_claim = new_claim_dict[property_id][0]

                    claims = existing_item.setdefault("claims", {})
                    claims.setdefault(property_id, []).append(new_claim)

            items.append(existing_item)

        self._flush_items(items, new=False)


class KeepStrategy(UpdateStrategy):
    """Keep existing claims and append only missing properties."""

    def run(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
        context: MappingContext,
    ) -> None:
        if dataframe.empty or not item_mapping.statements:
            return

        kept_claims = 0
        appended_claims = 0

        contexts = self._prepare_row_contexts(dataframe, item_mapping)
        if not contexts:
            return

        keys = self._collect_lookup_keys(contexts)
        with ItemBulkSearcher() as item_searcher:
            items_by_key = item_searcher.find_items(
                keys,
                language=context.language,
            )
        items: list[dict] = []

        for row_ctx in contexts:
            key = (row_ctx.label, row_ctx.normalized_description)
            existing_item = items_by_key.get(key) or items_by_key.get((row_ctx.label, None))
            if not existing_item or not existing_item.get("id"):
                continue

            current_claims = existing_item.get("claims") or {}
            modified_item = None

            for statement in item_mapping.statements:
                property_id = context.get_property_id(
                    statement.property_id, statement.property_label
                )
                if property_id in current_claims:
                    kept_claims += 1
                    continue

                new_claim_dict = self.claim_builder.build_claim(
                    statement, row_ctx.row, context
                )
                if not new_claim_dict:
                    continue

                if modified_item is None:
                    modified_item = copy.deepcopy(existing_item)
                    modified_item["labels"] = label(
                        context.language, row_ctx.label
                    )
                    if row_ctx.description_value:
                        modified_item["descriptions"] = description(
                            context.language, row_ctx.description_value
                        )
                    modified_item.setdefault("claims", current_claims)
                    current_claims = modified_item["claims"]

                current_claims[property_id] = new_claim_dict[property_id]
                appended_claims += 1

            if modified_item:
                items.append(modified_item)

        self._flush_items(items, new=False)

        print(
            f"KEEP action summary: kept {kept_claims} existing claims, "
            f"appended {appended_claims} new claims."
        )


class MergeRefsOrAppendStrategy(UpdateStrategy):
    """Placeholder for future implementation."""

    def run(
        self,
        dataframe: pd.DataFrame,
        item_mapping: ItemMapping,
        context: MappingContext,
    ) -> None:
        raise NotImplementedError("MERGE_REFS_OR_APPEND strategy not implemented yet.")


class UpdateStrategyFactory:
    """Small factory to resolve the correct update strategy."""

    STRATEGY_MAP: dict[
        UpdateAction,
        Type[UpdateStrategy],
    ] = {
        UpdateAction.REPLACE_ALL: ReplaceAllStrategy,
        UpdateAction.APPEND_OR_REPLACE: AppendOrReplaceStrategy,
        UpdateAction.FORCE_APPEND: ForceAppendStrategy,
        UpdateAction.KEEP: KeepStrategy,
        UpdateAction.MERGE_REFS_OR_APPEND: MergeRefsOrAppendStrategy,
    }

    @classmethod
    def for_mapping(
        cls,
        csv_config: CSVFileConfig,
        item_mapping: ItemMapping,
        claim_builder: ClaimBuilder | None = None,
    ) -> UpdateStrategy | None:
        action = item_mapping.update_action or csv_config.update_action
        if not action:
            return None
        strategy_cls = cls.STRATEGY_MAP.get(action)
        if not strategy_cls:
            return None
        return strategy_cls(claim_builder=claim_builder)
