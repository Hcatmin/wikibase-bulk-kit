"""Claim construction utilities that leverage the value resolver."""

from __future__ import annotations

import pandas as pd

from RaiseWikibase.datamodel import claim, snak, entity, label, description

from ..models import StatementDefinition
from .context import MappingContext
from .value_resolution import ValueResolver


class ClaimBuilder:
    """Builds snaks and claims for statements, qualifiers, and references."""

    def __init__(self, value_resolver: ValueResolver | None = None) -> None:
        self.value_resolver = value_resolver or ValueResolver()

    def _create_snak(
        self,
        claim_mapping: StatementDefinition,
        row: pd.Series,
        context: MappingContext,
    ) -> dict:
        property_id, datatype = context.get_property_info(claim_mapping.property)
        if not property_id:
            raise ValueError(f"Property not found: {claim_mapping.property}")

        value = self.value_resolver.resolve(
            claim_mapping.value,
            row,
            datatype,
            context,
        )

        return snak(
            datatype=datatype or "",
            value=value,
            prop=property_id,
            snaktype="value",
        )

    def build_claim(
        self,
        statement: StatementDefinition,
        row: pd.Series,
        context: MappingContext,
    ) -> dict | None:
        property_id, datatype = context.get_property_info(statement.property)
        if not property_id:
            return None

        try:
            value = self.value_resolver.resolve(
                statement.value,
                row,
                datatype,
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
            datatype=datatype or "",
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
        statements: list[StatementDefinition] | None,
        context: MappingContext,
    ) -> None:
        if not statements:
            return

        for statement in statements:
            claim_dict = self.build_claim(statement, row, context)
            if not claim_dict:
                continue
            item["claims"].update(claim_dict)
