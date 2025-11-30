"""Update strategies and creation helpers for the mapping pipeline."""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from typing import Type, Any, NamedTuple

import pandas as pd

from RaiseWikibase.datamodel import entity, label, description
from RaiseWikibase.raiser import batch
from RaiseWikibase.utils import is_same_claim

from ..models import MappingRule, UpdateAction, StatementDefinition, CSVFileConfig
from .context import MappingContext
from .claim_builder import ClaimBuilder
from wbk.processor.bulk_item_search import ItemBulkSearcher


class BatchMixin:
    """Utility mixin for batching operations."""

    def _flush_items(self, items: list[dict], new: bool) -> None:
        if not items:
            return
        batch("wikibase-item", items, new=new)
        items.clear()


class CreateItemsStep(BatchMixin):
    """Handles creation of new items in chunks."""

    def __init__(self, claim_builder: ClaimBuilder | None = None) -> None:
        self.claim_builder = claim_builder or ClaimBuilder()

    def _maybe_build_unique_key_statement(
        self, mapping_rule: MappingRule
    ) -> StatementDefinition | None:
        uk = mapping_rule.item.unique_key
        if not uk:
            return None
        if any(
            stmt.property == uk.property for stmt in (mapping_rule.statements or [])
        ):
            return None
        return StatementDefinition(property=uk.property, value=uk.value)

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        if dataframe.empty:
            return

        statements = list(mapping_rule.statements or [])
        extra_uk = self._maybe_build_unique_key_statement(mapping_rule)
        if extra_uk:
            statements.append(extra_uk)

        items: list[dict] = []
        for _, row in dataframe.iterrows():
            item_label = str(row.get("__label", ""))
            labels = label(context.language, item_label)

            descriptions = {}
            description_value = row.get("__description") if "__description" in row else None
            if pd.notna(description_value):
                descriptions = description(context.language, str(description_value))

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
                statements,
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
        unique_value: Any | None
        description_value: Any | None
        qid: str | None

    def __init__(self, claim_builder: ClaimBuilder | None = None) -> None:
        self.claim_builder = claim_builder or ClaimBuilder()

    @abstractmethod
    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        raise NotImplementedError

    def _prepare_row_contexts(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
    ) -> list["UpdateStrategy.RowContext"]:
        if dataframe.empty:
            return []

        contexts: list[UpdateStrategy.RowContext] = []
        for _, row in dataframe.iterrows():
            label_value = str(row.get("__label", ""))
            unique_value = row.get("__unique_key_value")
            description_value = row.get("__description") if "__description" in row else None
            qid = row.get("__qid")
            contexts.append(
                UpdateStrategy.RowContext(
                    row=row,
                    label=label_value,
                    unique_value=unique_value,
                    description_value=description_value,
                    qid=qid,
                )
            )
        return contexts

    def _build_unique_key_statement(self, mapping_rule: MappingRule) -> StatementDefinition | None:
        uk = mapping_rule.item.unique_key
        if not uk:
            return None
        if any(stmt.property == uk.property for stmt in (mapping_rule.statements or [])):
            return None
        return StatementDefinition(property=uk.property, value=uk.value)

    def _statements_with_unique_key(self, mapping_rule: MappingRule) -> list[StatementDefinition]:
        statements = list(mapping_rule.statements or [])
        extra = self._build_unique_key_statement(mapping_rule)
        if extra:
            statements.append(extra)
        return statements

    @staticmethod
    def _normalize_qid(qid: Any | None) -> str | None:
        if qid is None or pd.isna(qid):
            return None
        qid_str = str(qid).strip()
        if not qid_str:
            return None
        if not qid_str.upper().startswith("Q"):
            return f"Q{qid_str}"
        if qid_str.startswith("q"):
            return f"Q{qid_str[1:]}"
        return qid_str


class ReplaceAllStrategy(UpdateStrategy):
    """Replace the full set of claims for existing items."""

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        if dataframe.empty:
            return

        statements = self._statements_with_unique_key(mapping_rule)
        items: list[dict] = []
        for _, row in dataframe.iterrows():
            qid = self._normalize_qid(row.get("__qid"))
            if not qid:
                continue

            label_value = str(row.get("__label", ""))
            labels = label(context.language, label_value)
            description_value = row.get("__description") if "__description" in row else None
            descriptions = (
                description(context.language, str(description_value))
                if description_value not in (None, "")
                else {}
            )

            item = entity(
                labels=labels,
                aliases={},
                descriptions=descriptions,
                claims={},
                etype="item",
            )
            item["id"] = qid

            self.claim_builder.apply_statements(item, row, statements, context)
            items.append(item)

        self._flush_items(items, new=False)


class AppendOrReplaceStrategy(UpdateStrategy):
    """Append or replace claims depending on existing values."""

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        if dataframe.empty:
            return

        contexts = self._prepare_row_contexts(dataframe, mapping_rule)
        if not contexts:
            return

        qids = [
            qid
            for qid in (
                self._normalize_qid(ctx.qid) for ctx in contexts
            )
            if qid
        ]
        if not qids:
            return

        with ItemBulkSearcher() as item_searcher:
            items_by_qid = item_searcher.find_items_by_qids(
                qids,
                language=context.language,
            )

        statements = self._statements_with_unique_key(mapping_rule)
        items: list[dict] = []
        for row_ctx in contexts:
            qid = self._normalize_qid(row_ctx.qid)
            if not qid:
                continue

            existing_item = items_by_qid.get(qid)
            if not existing_item:
                continue

            existing_item = copy.deepcopy(existing_item)
            existing_item["labels"] = label(context.language, row_ctx.label)

            if row_ctx.description_value not in (None, ""):
                existing_item["descriptions"] = description(
                    context.language, str(row_ctx.description_value)
                )

            for statement in statements:
                new_claim_dict = self.claim_builder.build_claim(
                    statement, row_ctx.row, context
                )
                if not new_claim_dict:
                    continue

                property_id_stmt, _ = context.get_property_info(statement.property)
                if not property_id_stmt:
                    continue

                new_claim = new_claim_dict[property_id_stmt][0]

                claims = existing_item.setdefault("claims", {})
                if property_id_stmt in claims:
                    existing_claims = claims[property_id_stmt]
                    claim_found = False
                    for idx, existing_claim in enumerate(existing_claims):
                        if is_same_claim(new_claim, existing_claim):
                            if existing_claim.get("id"):
                                new_claim["id"] = existing_claim["id"]
                            existing_claims[idx] = new_claim
                            claim_found = True
                            break

                    if not claim_found:
                        existing_claims.append(new_claim)
                else:
                    claims[property_id_stmt] = [new_claim]

            items.append(existing_item)

        self._flush_items(items, new=False)


class ForceAppendStrategy(UpdateStrategy):
    """Always append new claims even if duplicates exist."""

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        if dataframe.empty:
            return

        contexts = self._prepare_row_contexts(dataframe, mapping_rule)
        if not contexts:
            return

        qids = [
            qid
            for qid in (
                self._normalize_qid(ctx.qid) for ctx in contexts
            )
            if qid
        ]
        if not qids:
            return

        with ItemBulkSearcher() as item_searcher:
            items_by_qid = item_searcher.find_items_by_qids(
                qids,
                language=context.language,
            )

        statements = self._statements_with_unique_key(mapping_rule)
        items: list[dict] = []
        for row_ctx in contexts:
            qid = self._normalize_qid(row_ctx.qid)
            if not qid:
                continue

            existing_item = items_by_qid.get(qid)
            if not existing_item:
                continue

            existing_item = copy.deepcopy(existing_item)
            existing_item["labels"] = label(context.language, row_ctx.label)

            if row_ctx.description_value not in (None, ""):
                existing_item["descriptions"] = description(
                    context.language, str(row_ctx.description_value)
                )

            for statement in statements:
                new_claim_dict = self.claim_builder.build_claim(
                    statement, row_ctx.row, context
                )
                if not new_claim_dict:
                    continue

                property_id_stmt, _ = context.get_property_info(statement.property)
                if not property_id_stmt:
                    continue

                new_claim = new_claim_dict[property_id_stmt][0]

                claims = existing_item.setdefault("claims", {})
                claims.setdefault(property_id_stmt, []).append(new_claim)

            items.append(existing_item)

        self._flush_items(items, new=False)


class KeepStrategy(UpdateStrategy):
    """Keep existing claims and append only missing properties."""

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        if dataframe.empty or not mapping_rule.statements:
            return

        contexts = self._prepare_row_contexts(dataframe, mapping_rule)
        if not contexts:
            return

        qids = [
            qid
            for qid in (
                self._normalize_qid(ctx.qid) for ctx in contexts
            )
            if qid
        ]
        if not qids:
            return

        with ItemBulkSearcher() as item_searcher:
            items_by_qid = item_searcher.find_items_by_qids(
                qids,
                language=context.language,
            )

        statements = self._statements_with_unique_key(mapping_rule)
        items: list[dict] = []
        kept_claims = 0
        appended_claims = 0

        for row_ctx in contexts:
            qid = self._normalize_qid(row_ctx.qid)
            if not qid:
                continue

            existing_item = items_by_qid.get(qid)
            if not existing_item or not existing_item.get("id"):
                continue

            current_claims = existing_item.get("claims") or {}
            modified_item = None

            for statement in statements:
                property_id_stmt, _ = context.get_property_info(statement.property)
                if not property_id_stmt:
                    continue

                if property_id_stmt in current_claims:
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
                    if row_ctx.description_value not in (None, ""):
                        modified_item["descriptions"] = description(
                            context.language, str(row_ctx.description_value)
                        )
                    modified_item.setdefault("claims", current_claims)
                    current_claims = modified_item["claims"]

                current_claims[property_id_stmt] = new_claim_dict[property_id_stmt]
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
        mapping_rule: MappingRule,
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
        mapping_rule: MappingRule,
        claim_builder: ClaimBuilder | None = None,
    ) -> UpdateStrategy | None:
        action = mapping_rule.update_action or csv_config.update_action
        if not action:
            return None
        strategy_cls = cls.STRATEGY_MAP.get(action)
        if not strategy_cls:
            return None
        return strategy_cls(claim_builder=claim_builder)
