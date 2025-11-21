"""Claim construction utilities that leverage the value resolver."""

from __future__ import annotations

import pandas as pd

from RaiseWikibase.datamodel import claim, snak, entity, label, description

from ..models import StatementMapping, ClaimMapping
from .context import MappingContext
from .value_resolution import ValueResolver


class ClaimBuilder:
    """Builds snaks and claims for statements, qualifiers, and references."""

    def __init__(self, value_resolver: ValueResolver | None = None) -> None:
        self.value_resolver = value_resolver or ValueResolver()

    def _create_snak(
        self,
        claim_mapping: ClaimMapping,
        row: pd.Series,
        context: MappingContext,
    ) -> dict:
        property_id = context.get_property_id(
            claim_mapping.property_id,
            claim_mapping.property_label,
        )

        value = self.value_resolver.resolve(
            claim_mapping.value,
            row,
            claim_mapping.datatype,
            context,
        )

        return snak(
            datatype=claim_mapping.datatype,
            value=value,
            prop=property_id,
            snaktype="value",
        )

    def build_claim(
        self,
        statement: StatementMapping,
        row: pd.Series,
        context: MappingContext,
    ) -> dict | None:
        property_id = context.get_property_id(
            statement.property_id,
            statement.property_label,
        )

        try:
            value = self.value_resolver.resolve(
                statement.value,
                row,
                statement.datatype,
                context,
            )
        except ValueError:
            return None

        if isinstance(value, tuple) and any(part == " " for part in value):
            return None

        qualifiers: list[dict] = []
        if statement.qualifiers:
            for qualifier in statement.qualifiers:
                qualifiers.append(self._create_snak(qualifier, row, context))

        references: list[dict] = []
        if statement.references:
            for reference in statement.references:
                references.append(self._create_snak(reference, row, context))

        mainsnak_dict = snak(
            datatype=statement.datatype,
            value=value,
            prop=property_id,
            snaktype="value",
        )

        if not mainsnak_dict:
            return None

        claim_dict = claim(
            prop=property_id,
            mainsnak=mainsnak_dict,
            qualifiers=qualifiers,
            references=references,
        )

        if statement.rank and statement.rank != "normal":
            claim_dict[property_id][0]["rank"] = statement.rank

        return claim_dict

    def apply_statements(
        self,
        item: entity,
        row: pd.Series,
        statements: list[StatementMapping] | None,
        context: MappingContext,
    ) -> None:
        if not statements:
            return

        for statement in statements:
            claim_dict = self.build_claim(statement, row, context)
            if not claim_dict:
                continue
            item["claims"].update(claim_dict)

    def build_labels_and_description(
        self,
        row: pd.Series,
        item_mapping,
        context: MappingContext,
        dataframe: pd.DataFrame,
    ):
        """Convenience helper kept for compatibility with existing flow."""
        item_name = str(row[item_mapping.label_column])
        labels = label(context.language, item_name)

        descriptions = {}
        if item_mapping.description and item_mapping.description in dataframe.columns:
            item_description = str(row[item_mapping.description])
            descriptions = description(context.language, item_description)

        return labels, descriptions
