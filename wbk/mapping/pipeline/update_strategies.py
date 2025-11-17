"""Update strategies and creation helpers for the mapping pipeline."""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from typing import Iterable, Type, Any

import pandas as pd

from RaiseWikibase.datamodel import entity, label, description
from RaiseWikibase.raiser import batch

from ..models import ItemMapping, UpdateAction, StatementMapping, CSVFileConfig
from .context import MappingContext
from .claim_builder import ClaimBuilder

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

            if len(items) >= self.chunk_size:
                self._flush_items(items, new=True)

        self._flush_items(items, new=True)
        context.verify_items_created()


class UpdateStrategy(BatchMixin, ABC):
    """Base class for update strategies."""

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

        total_rows = len(dataframe)
        for start in range(0, total_rows, self.chunk_size):
            chunk = dataframe.iloc[start : start + self.chunk_size]
            if chunk.empty:
                continue

            qids = context.item_searcher.find_items_by_labels_optimized(
                chunk[item_mapping.label_column].tolist()
            )

            chunk_items: list[dict] = []

            list_columns = self._get_columns(
                item_mapping.description,
                dataframe.columns
            ) if item_mapping.description else []
            for _, row in chunk.iterrows():
                item_label = str(row[item_mapping.label_column])
                qid = qids.get(item_label)
                if not qid:
                    continue

                labels = label(context.language, item_label)
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
                item["id"] = qid

                self.claim_builder.apply_statements(
                    item, row, item_mapping.statements, context
                )
                chunk_items.append(item)

            if chunk_items:
                self._flush_items(chunk_items, new=False)


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

        total_rows = len(dataframe)
        for start in range(0, total_rows, self.chunk_size):
            chunk = dataframe.iloc[start : start + self.chunk_size]
            if chunk.empty:
                continue

            items_by_label = (
                context.item_searcher.find_items_by_labels_with_data(
                    chunk[item_mapping.label_column].tolist(),
                    language=context.language,
                )
            )

            chunk_items: list[dict] = []

            for _, row in chunk.iterrows():
                item_label = str(row[item_mapping.label_column])
                existing_item = items_by_label.get(item_label)
                if not existing_item:
                    continue

                existing_item = copy.deepcopy(existing_item)
                existing_item["labels"] = label(context.language, item_label)

                if (
                    item_mapping.description
                    and item_mapping.description in chunk.columns
                ):
                    item_description = str(row[item_mapping.description])
                    existing_item["descriptions"] = description(
                        context.language, item_description
                    )

                if item_mapping.statements:
                    for statement in item_mapping.statements:
                        new_claim_dict = self.claim_builder.build_claim(
                            statement, row, context
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

                chunk_items.append(existing_item)

            if chunk_items:
                self._flush_items(chunk_items, new=False)


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

        total_rows = len(dataframe)
        for start in range(0, total_rows, self.chunk_size):
            chunk = dataframe.iloc[start : start + self.chunk_size]
            if chunk.empty:
                continue

            items_by_label = (
                context.item_searcher.find_items_by_labels_with_data(
                    chunk[item_mapping.label_column].tolist(),
                    language=context.language,
                )
            )

            chunk_items: list[dict] = []
            for _, row in chunk.iterrows():
                item_label = str(row[item_mapping.label_column])
                existing_item = items_by_label.get(item_label)
                if not existing_item:
                    continue

                existing_item = copy.deepcopy(existing_item)
                existing_item["labels"] = label(context.language, item_label)

                if (
                    item_mapping.description
                    and item_mapping.description in chunk.columns
                ):
                    item_description = str(row[item_mapping.description])
                    existing_item["descriptions"] = description(
                        context.language, item_description
                    )

                if item_mapping.statements:
                    for statement in item_mapping.statements:
                        new_claim_dict = self.claim_builder.build_claim(
                            statement, row, context
                        )
                        if not new_claim_dict:
                            continue

                        property_id = context.get_property_id(
                            statement.property_id, statement.property_label
                        )
                        new_claim = new_claim_dict[property_id][0]

                        claims = existing_item.setdefault("claims", {})
                        claims.setdefault(property_id, []).append(new_claim)

                chunk_items.append(existing_item)

            if chunk_items:
                self._flush_items(chunk_items, new=False)


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

        total_rows = len(dataframe)
        for start in range(0, total_rows, self.chunk_size):
            chunk = dataframe.iloc[start : start + self.chunk_size]
            if chunk.empty:
                continue

            items_by_label = (
                context.item_searcher.find_items_by_labels_with_data(
                    chunk[item_mapping.label_column].tolist(),
                    language=context.language,
                )
            )

            chunk_items: list[dict] = []

            for _, row in chunk.iterrows():
                item_label = str(row[item_mapping.label_column])
                existing_item = items_by_label.get(item_label)
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
                        statement, row, context
                    )
                    if not new_claim_dict:
                        continue

                    if modified_item is None:
                        modified_item = copy.deepcopy(existing_item)
                        modified_item["labels"] = label(
                            context.language, item_label
                        )
                        if (
                            item_mapping.description
                            and item_mapping.description in chunk.columns
                        ):
                            item_description = str(row[item_mapping.description])
                            modified_item["descriptions"] = description(
                                context.language, item_description
                            )
                        modified_item.setdefault("claims", current_claims)
                        current_claims = modified_item["claims"]

                    current_claims[property_id] = new_claim_dict[property_id]
                    appended_claims += 1

                if modified_item:
                    chunk_items.append(modified_item)

            if chunk_items:
                self._flush_items(chunk_items, new=False)

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
