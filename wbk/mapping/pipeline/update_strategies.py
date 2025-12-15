"""Update strategies and creation helpers for the mapping pipeline."""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from typing import Type, Any, NamedTuple

import pandas as pd

from RaiseWikibase.datamodel import entity, label, description
from RaiseWikibase.raiser import batch
from RaiseWikibase.utils import is_same_claim, is_same_snak

from ..models import MappingRule, UpdateAction, StatementDefinition, CSVFileConfig
from .context import MappingContext
from .claim_builder import ClaimBuilder


class BatchMixin:
    """Utility mixin for batching operations."""

    def _flush_items(self, items: list[dict], new: bool) -> None:
        if not items:
            return
        batch("wikibase-item", items, new=new)
        items.clear()

    def _set_labels_and_descriptions(self, item: dict, row: pd.Series, language: str) -> None:
        if (new_label_value := row.get("__new_label")) is not None:
            item["labels"][language] = {
                "language": language,
                "value": str(new_label_value)
            }
        elif (label_value := row.get("__label")) is not None:
            item["labels"][language] = {
                "language": language, 
                "value": str(label_value)
            }
        if (new_description_value := row.get("__new_description")) is not None:
            item["descriptions"][language] = {
                "language": language,
                "value": str(new_description_value)
            }
        elif (description_value := row.get("__description")) is not None:
            item["descriptions"][language] = {
                "language": language,
                "value": str(description_value)
            }
            

class CreateItemsStep(BatchMixin):
    """Handles creation of new items in chunks."""

    def __init__(self, claim_builder: ClaimBuilder | None = None) -> None:
        self.claim_builder = claim_builder or ClaimBuilder()

    def _maybe_build_snak_statement(
        self, mapping_rule: MappingRule
    ) -> StatementDefinition | None:
        """Build a statement from the item's statement matcher if not already present."""
        snak = mapping_rule.item.snak
        if not snak:
            return None
        if any(
            stmt.property == snak.property for stmt in (mapping_rule.statements or [])
        ):
            return None
        return StatementDefinition(property=snak.property, value=snak.value)

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        if dataframe.empty:
            return

        statements = list(mapping_rule.statements or [])
        extra_snak = self._maybe_build_snak_statement(mapping_rule)
        if extra_snak:
            statements.append(extra_snak)

        items: list[dict] = []
        for _, row in dataframe.iterrows():
            item = entity(
                labels={},
                aliases={},
                descriptions={},
                claims={},
                etype="item",
            )
            self._set_labels_and_descriptions(item, row, context.language)

            self.claim_builder.apply_statements(
                item,
                row,
                statements,
                context,
            )
            items.append(item)

        self._flush_items(items, new=True)


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
        self._working_items: dict[str, dict] = {}

    @abstractmethod
    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        raise NotImplementedError

    def _build_snak_statement(
        self, mapping_rule: MappingRule
    ) -> StatementDefinition | None:
        """Build a statement from the item's statement matcher if not already present."""
        snak = mapping_rule.item.snak
        if not snak:
            return None
        if any(
            stmt.property == snak.property for stmt in (mapping_rule.statements or [])
        ):
            return None
        return StatementDefinition(property=snak.property, value=snak.value)

    def _statements_with_snak(
        self, mapping_rule: MappingRule
    ) -> list[StatementDefinition]:
        """Get statements with the snak statement appended if not already present."""
        statements = list(mapping_rule.statements or [])
        extra = self._build_snak_statement(mapping_rule)
        if extra:
            statements.append(extra)
        return statements

    def _get_or_init_item(
        self,
        row: pd.Series,
        context: MappingContext,
    ) -> dict | None:
        """Return a working item dict for this row, initializing once per qid."""
        qid = row.get("__qid")
        if qid is None and isinstance(row.get("__item"), dict):
            qid = row["__item"].get("id")
        if not qid:
            return None

        if qid not in self._working_items:
            base_item = context.get_item(qid) or row.get("__item")
            if not base_item:
                return None
            self._working_items[qid] = copy.deepcopy(base_item)

        return self._working_items[qid]

    def _flush_working_items(self, new: bool) -> None:
        """Flush all accumulated working items and clear cache."""
        self._flush_items(list(self._working_items.values()), new=new)
        self._working_items.clear()

    def _reset_working_items(self) -> None:
        """Clear working item cache at the start of a run."""
        self._working_items.clear()


class ReplaceAllStrategy(UpdateStrategy):
    """Replace the full set of claims for existing items."""

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        self._reset_working_items()
        if dataframe.empty:
            return

        statements = self._statements_with_snak(mapping_rule)
        for _, row in dataframe.iterrows():
            item = self._get_or_init_item(row, context)
            if not item:
                continue

            self._set_labels_and_descriptions(item, row, context.language)

            item["claims"] = {}

            self.claim_builder.apply_statements(item, row, statements, context)

        self._flush_working_items(new=False)


class AppendOrReplaceStrategy(UpdateStrategy):
    """Append or replace claims depending on existing values."""

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        self._reset_working_items()
        if dataframe.empty:
            return

        statements = self._statements_with_snak(mapping_rule)
        for _, row in dataframe.iterrows():
            existing_item = self._get_or_init_item(row, context)
            if not existing_item:
                continue

            self._set_labels_and_descriptions(existing_item, row, context.language)

            for statement in statements:
                new_claim_dict = self.claim_builder.build_claim(
                    statement, row, context
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

        self._flush_working_items(new=False)


class ForceAppendStrategy(UpdateStrategy):
    """Always append new claims even if duplicates exist."""

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        self._reset_working_items()
        if dataframe.empty:
            return

        statements = self._statements_with_snak(mapping_rule)
        for _, row in dataframe.iterrows():
            existing_item = self._get_or_init_item(row, context)
            if not existing_item:
                continue

            self._set_labels_and_descriptions(existing_item, row, context.language)

            for statement in statements:
                new_claim_dict = self.claim_builder.build_claim(
                    statement, row, context
                )
                if not new_claim_dict:
                    continue

                property_id_stmt, _ = context.get_property_info(statement.property)
                if not property_id_stmt:
                    continue

                new_claim = new_claim_dict[property_id_stmt][0]

                claims = existing_item.setdefault("claims", {})
                claims.setdefault(property_id_stmt, []).append(new_claim)

        self._flush_working_items(new=False)


class KeepStrategy(UpdateStrategy):
    """Keep existing claims and append only missing properties."""

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        self._reset_working_items()
        if dataframe.empty or not mapping_rule.statements:
            return

        statements = self._statements_with_snak(mapping_rule)
        kept_claims = 0
        appended_claims = 0

        for _, row in dataframe.iterrows():
            existing_item = self._get_or_init_item(row, context)
            if not existing_item:
                continue

            self._set_labels_and_descriptions(existing_item, row, context.language)

            # Ensure claims dict exists and get reference for modifications
            current_claims = existing_item.setdefault("claims", {})

            for statement in statements:
                property_id_stmt, _ = context.get_property_info(statement.property)
                if not property_id_stmt:
                    continue

                # Check if property exists AND has claims
                if current_claims.get(property_id_stmt):
                    kept_claims += 1
                    continue

                new_claim_dict = self.claim_builder.build_claim(
                    statement, row, context
                )
                if not new_claim_dict:
                    continue

                current_claims[property_id_stmt] = new_claim_dict[property_id_stmt]
                appended_claims += 1

        self._flush_working_items(new=False)

        print(
            f"KEEP action summary: kept {kept_claims} existing claims, "
            f"appended {appended_claims} new claims."
        )


class MergeRefsOrAppendStrategy(UpdateStrategy):
    """Merge references if claim exists, otherwise append.
    
    This strategy is useful when aggregating data from multiple sources.
    It will:
    1. Find existing claims with matching mainsnak value and qualifiers
    2. Merge new references into existing claims if they don't already exist
    3. Append new claims if no matching claim is found
    
    Example:
        Existing claim: Population=50000 (date=2020-01-01, ref=Q456)
        New data: Population=50000 (date=2020-01-01, ref=Q789)
        Result: Same claim with both references (Q456, Q789)
        
        New data: Population=52000 (date=2021-01-01, ref=Q456)
        Result: New claim appended (different value/qualifiers)
    """

    def _mainsnak_values_equal(self, claim1: dict, claim2: dict) -> bool:
        """Check if mainsnak datavalues are equal."""
        mainsnak1 = claim1.get("mainsnak", {})
        mainsnak2 = claim2.get("mainsnak", {})
        
        datavalue1 = mainsnak1.get("datavalue", {})
        datavalue2 = mainsnak2.get("datavalue", {})
        
        return datavalue1.get("value") == datavalue2.get("value")

    def _qualifiers_equal(self, claim1: dict, claim2: dict) -> bool:
        """Check if qualifiers are equal between two claims."""
        qualifiers1 = claim1.get("qualifiers", {})
        qualifiers2 = claim2.get("qualifiers", {})
        
        # Compare qualifiers-order
        order1 = claim1.get("qualifiers-order", [])
        order2 = claim2.get("qualifiers-order", [])
        if set(order1) != set(order2):
            return False
        
        # Compare qualifiers for each property
        for prop in order1:
            quals1 = qualifiers1.get(prop, [])
            quals2 = qualifiers2.get(prop, [])
            
            if len(quals1) != len(quals2):
                return False
            
            # Compare each qualifier snak
            for q1, q2 in zip(quals1, quals2):
                if not is_same_snak(q1, q2):
                    return False
        
        return True

    def _ref_present(self, new_claim: dict, existing_claim: dict) -> bool:
        """Check if the new claim's reference block is present in existing claim."""
        new_refs = new_claim.get("references", [])
        existing_refs = existing_claim.get("references", [])
        
        if not new_refs:
            return True  # No references to check
        
        for new_ref in new_refs:
            new_snaks = new_ref.get("snaks", {})
            new_snaks_order = new_ref.get("snaks-order", [])
            
            # Check if this reference block exists in existing references
            for existing_ref in existing_refs:
                existing_snaks = existing_ref.get("snaks", {})
                existing_snaks_order = existing_ref.get("snaks-order", [])
                
                # Check if snaks-order matches
                if set(new_snaks_order) != set(existing_snaks_order):
                    continue
                
                # Check if all snaks match
                all_match = True
                for prop in new_snaks_order:
                    new_prop_snaks = new_snaks.get(prop, [])
                    existing_prop_snaks = existing_snaks.get(prop, [])
                    
                    if len(new_prop_snaks) != len(existing_prop_snaks):
                        all_match = False
                        break
                    
                    for ns, es in zip(new_prop_snaks, existing_prop_snaks):
                        if not is_same_snak(ns, es):
                            all_match = False
                            break
                    
                    if not all_match:
                        break
                
                if all_match:
                    return True
        
        return False

    def _merge_references(self, existing_claim: dict, new_claim: dict) -> None:
        """Merge references from new_claim into existing_claim if not present."""
        new_refs = new_claim.get("references", [])
        existing_refs = existing_claim.get("references", [])
        
        if not new_refs:
            return
        
        for new_ref in new_refs:
            new_snaks = new_ref.get("snaks", {})
            new_snaks_order = new_ref.get("snaks-order", [])
            
            # Check if this reference already exists
            ref_exists = False
            for existing_ref in existing_refs:
                existing_snaks = existing_ref.get("snaks", {})
                existing_snaks_order = existing_ref.get("snaks-order", [])
                
                if set(new_snaks_order) != set(existing_snaks_order):
                    continue
                
                all_match = True
                for prop in new_snaks_order:
                    new_prop_snaks = new_snaks.get(prop, [])
                    existing_prop_snaks = existing_snaks.get(prop, [])
                    
                    if len(new_prop_snaks) != len(existing_prop_snaks):
                        all_match = False
                        break
                    
                    for ns, es in zip(new_prop_snaks, existing_prop_snaks):
                        if not is_same_snak(ns, es):
                            all_match = False
                            break
                    
                    if not all_match:
                        break
                
                if all_match:
                    ref_exists = True
                    break
            
            # If reference doesn't exist, add it
            if not ref_exists:
                existing_refs.append(new_ref)

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        self._reset_working_items()
        if dataframe.empty:
            return

        statements = self._statements_with_snak(mapping_rule)
        
        for _, row in dataframe.iterrows():
            existing_item = self._get_or_init_item(row, context)
            if not existing_item:
                continue

            self._set_labels_and_descriptions(existing_item, row, context.language)

            for statement in statements:
                new_claim_dict = self.claim_builder.build_claim(
                    statement, row, context
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
                    
                    for existing_claim in existing_claims:
                        # Check if mainsnak values match
                        if not self._mainsnak_values_equal(new_claim, existing_claim):
                            continue
                        
                        # Check if qualifiers match
                        if not self._qualifiers_equal(new_claim, existing_claim):
                            continue
                        
                        # Claim with same value and qualifiers found
                        claim_found = True
                        
                        # Check if references are already present
                        if not self._ref_present(new_claim, existing_claim):
                            # Merge references into existing claim
                            self._merge_references(existing_claim, new_claim)
                        
                        # Keep existing claim (don't replace it)
                        break

                    # If claim doesn't exist, append it
                    if not claim_found:
                        existing_claims.append(new_claim)
                else:
                    claims[property_id_stmt] = [new_claim]

        self._flush_working_items(new=False)


class MergeQualifiersStrategy(UpdateStrategy):
    """Merge qualifiers if claim exists with same value, otherwise append.
    
    This strategy merges qualifier values when claims have the same property
    and mainsnak value, even if qualifiers differ. It will:
    1. Find existing claims with matching mainsnak value
    2. Merge new qualifier values into existing claims (deduplicating)
    3. Merge references if they don't already exist
    4. Append new claims if no matching claim is found
    
    Example:
        Existing claim: 
            property="personas matrículadas", value=7
            qualifiers: año=2020, identificador género=mujer
        
        New data: 
            property="personas matrículadas", value=7
            qualifiers: año=2021, identificador género=mujer
        
        Result: Same claim with merged qualifiers:
            qualifiers: año=2020,2021, identificador género=mujer
    """

    def _mainsnak_values_equal(self, claim1: dict, claim2: dict) -> bool:
        """Check if mainsnak datavalues are equal."""
        mainsnak1 = claim1.get("mainsnak", {})
        mainsnak2 = claim2.get("mainsnak", {})
        
        datavalue1 = mainsnak1.get("datavalue", {})
        datavalue2 = mainsnak2.get("datavalue", {})
        
        return datavalue1.get("value") == datavalue2.get("value")

    def _qualifier_snak_exists(
        self, qualifier_snak: dict, qualifiers_list: list[dict]
    ) -> bool:
        """Check if a qualifier snak already exists in a list."""
        return any(is_same_snak(qualifier_snak, existing) 
                   for existing in qualifiers_list)

    def _merge_qualifiers(
        self, existing_claim: dict, new_claim: dict
    ) -> None:
        """Merge qualifiers from new_claim into existing_claim.
        
        This method merges qualifier values by adding new qualifier snaks
        to existing lists, preserving all existing qualifiers. It does NOT
        replace any existing qualifiers.
        """
        new_qualifiers = new_claim.get("qualifiers", {})
        if not new_qualifiers:
            return
        
        # Ensure existing_claim has qualifiers structure
        if "qualifiers" not in existing_claim:
            existing_claim["qualifiers"] = {}
        existing_qualifiers = existing_claim["qualifiers"]
        
        if "qualifiers-order" not in existing_claim:
            existing_claim["qualifiers-order"] = []
        existing_order = existing_claim["qualifiers-order"]
        
        new_order = new_claim.get("qualifiers-order", [])
        
        # Merge qualifiers for each property in the new claim
        for prop in new_order:
            new_prop_qualifiers = new_qualifiers.get(prop, [])
            if not new_prop_qualifiers:
                continue
            
            # Get the existing list for this qualifier property, or create new
            if prop in existing_qualifiers:
                # Use existing list - we'll append to it
                existing_prop_qualifiers = existing_qualifiers[prop]
            else:
                # Create new list for this property
                existing_prop_qualifiers = []
                existing_qualifiers[prop] = existing_prop_qualifiers
            
            # Add qualifiers that don't already exist (preserve existing)
            for new_qual in new_prop_qualifiers:
                if not self._qualifier_snak_exists(
                    new_qual, existing_prop_qualifiers
                ):
                    existing_prop_qualifiers.append(new_qual)
            
            # Ensure property is in qualifiers-order
            if prop not in existing_order:
                existing_order.append(prop)

    def _ref_present(self, new_claim: dict, existing_claim: dict) -> bool:
        """Check if the new claim's reference block is present."""
        new_refs = new_claim.get("references", [])
        existing_refs = existing_claim.get("references", [])
        
        if not new_refs:
            return True  # No references to check
        
        for new_ref in new_refs:
            new_snaks = new_ref.get("snaks", {})
            new_snaks_order = new_ref.get("snaks-order", [])
            
            # Check if this reference block exists
            for existing_ref in existing_refs:
                existing_snaks = existing_ref.get("snaks", {})
                existing_snaks_order = existing_ref.get("snaks-order", [])
                
                # Check if snaks-order matches
                if set(new_snaks_order) != set(existing_snaks_order):
                    continue
                
                # Check if all snaks match
                all_match = True
                for prop in new_snaks_order:
                    new_prop_snaks = new_snaks.get(prop, [])
                    existing_prop_snaks = existing_snaks.get(prop, [])
                    
                    if len(new_prop_snaks) != len(existing_prop_snaks):
                        all_match = False
                        break
                    
                    for ns, es in zip(new_prop_snaks, existing_prop_snaks):
                        if not is_same_snak(ns, es):
                            all_match = False
                            break
                    
                    if not all_match:
                        break
                
                if all_match:
                    return True
        
        return False

    def _merge_references(
        self, existing_claim: dict, new_claim: dict
    ) -> None:
        """Merge references from new_claim into existing_claim if not present."""
        new_refs = new_claim.get("references", [])
        existing_refs = existing_claim.setdefault("references", [])
        
        if not new_refs:
            return
        
        for new_ref in new_refs:
            new_snaks = new_ref.get("snaks", {})
            new_snaks_order = new_ref.get("snaks-order", [])
            
            # Check if this reference already exists
            ref_exists = False
            for existing_ref in existing_refs:
                existing_snaks = existing_ref.get("snaks", {})
                existing_snaks_order = existing_ref.get("snaks-order", [])
                
                if set(new_snaks_order) != set(existing_snaks_order):
                    continue
                
                all_match = True
                for prop in new_snaks_order:
                    new_prop_snaks = new_snaks.get(prop, [])
                    existing_prop_snaks = existing_snaks.get(prop, [])
                    
                    if len(new_prop_snaks) != len(existing_prop_snaks):
                        all_match = False
                        break
                    
                    for ns, es in zip(new_prop_snaks, existing_prop_snaks):
                        if not is_same_snak(ns, es):
                            all_match = False
                            break
                    
                    if not all_match:
                        break
                
                if all_match:
                    ref_exists = True
                    break
            
            # If reference doesn't exist, add it
            if not ref_exists:
                existing_refs.append(new_ref)

    def run(
        self,
        dataframe: pd.DataFrame,
        mapping_rule: MappingRule,
        context: MappingContext,
    ) -> None:
        self._reset_working_items()
        if dataframe.empty:
            return

        statements = self._statements_with_snak(mapping_rule)
        
        for _, row in dataframe.iterrows():
            existing_item = self._get_or_init_item(row, context)
            if not existing_item:
                continue

            self._set_labels_and_descriptions(existing_item, row, context.language)

            for statement in statements:
                new_claim_dict = self.claim_builder.build_claim(
                    statement, row, context
                )
                if not new_claim_dict:
                    continue

                property_id_stmt, _ = context.get_property_info(
                    statement.property
                )
                if not property_id_stmt:
                    continue

                new_claim = new_claim_dict[property_id_stmt][0]

                claims = existing_item.setdefault("claims", {})
                if property_id_stmt in claims:
                    existing_claims = claims[property_id_stmt]
                    claim_found = False
                    
                    for existing_claim in existing_claims:
                        # Check if mainsnak values match
                        if not self._mainsnak_values_equal(
                            new_claim, existing_claim
                        ):
                            continue
                        
                        # Claim with same value found - merge qualifiers
                        claim_found = True
                        
                        # Merge qualifiers from new claim
                        self._merge_qualifiers(existing_claim, new_claim)
                        
                        # Merge references if not already present
                        if not self._ref_present(new_claim, existing_claim):
                            self._merge_references(existing_claim, new_claim)
                        
                        # Keep existing claim (don't replace it)
                        break

                    # If claim doesn't exist, append it
                    if not claim_found:
                        existing_claims.append(new_claim)
                else:
                    claims[property_id_stmt] = [new_claim]

        self._flush_working_items(new=False)


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
        UpdateAction.MERGE_QUALIFIERS_OR_APPEND: MergeQualifiersStrategy,
    }

    @classmethod
    def for_mapping(
        cls,
        csv_config: CSVFileConfig,
        mapping_rule: MappingRule,
        claim_builder: ClaimBuilder | None = None,
        action: UpdateAction | None = None,
    ) -> UpdateStrategy | None:
        action = action or mapping_rule.update_action or csv_config.update_action
        if not action:
            return None
        strategy_cls = cls.STRATEGY_MAP.get(action)
        if not strategy_cls:
            return None
        return strategy_cls(claim_builder=claim_builder)
