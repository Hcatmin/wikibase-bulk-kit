import re
import sys
from typing import Optional, List, Any
from rich.console import Console

from wikibaseintegrator import wbi_helpers
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.models import Qualifiers, References
from wikibaseintegrator.datatypes import (
    String, ExternalID, Time, Quantity, Item, URL, CommonsMedia
)

from ..config.manager import ConfigManager
from ..schema.models import PropertySchema, ItemSchema, StatementSchema, ClaimSchema
from .interface import BackendStrategy

console = Console(force_terminal=True, width=120)
stderr_console = Console(file=sys.stderr, force_terminal=True, width=120)

class ApiBackend(BackendStrategy):
    """Backend strategy using WikibaseIntegrator API."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.wbi = config_manager.get_wikibase_integrator()
        # Cache for items by label and description to avoid repeated lookups
        self.items_by_label_and_description: dict[str, dict[str, str]] = {}
        # Cache for properties by label
        self.properties_by_label: dict[str, str] = {}

    def find_property_by_label(self, label: str, language: str) -> Optional[str]:
        properties = wbi_helpers.search_entities(label, language=language, search_type='property', dict_result=True)

        for prop in properties:
            prop_id = prop.get('id', 'Unknown')
            try:
                full_prop = self.wbi.property.get(entity_id=prop_id)
                prop_label = full_prop.labels.get(language)
                
                if prop_label and prop_label.value == label:
                    return prop_id
            except Exception:
                continue
        
        return None

    def find_item_by_label(self, label: str, language: str) -> Optional[str]:
        response = wbi_helpers.search_entities(label, language=language, search_type='item', dict_result=True)

        if len(response) == 0:
            raise ValueError(f"No item found for label '{label}'")
        elif len(response) > 1:
            raise ValueError(f"Multiple items found for label '{label}'. Disambiguate.")
        else:
            return response[0].get('id')

    def find_item_by_label_and_description(self, label: str, description: str, language: str) -> Optional[str]:
        items = wbi_helpers.search_entities(label, language=language, search_type='item', dict_result=True)
        
        for item in items:
            item_id = item.get('id')
            try:
                full_item = self.wbi.item.get(entity_id=item_id)
                item_label = full_item.labels.get(language)
                item_description = full_item.descriptions.get(language)
        
                if (item_label and item_label.value == label and 
                    item_description and item_description.value == description):
                    return item_id
            except Exception:
                continue
        
        return None

    def find_item_by_expression(self, expression: str, language: str) -> Optional[str]:
        if re.match(r'.+ \(.+\)$', expression):
            label = expression.split('(')[0].strip()
            key_word = expression.split('(')[1].split(')')[0].strip()
            
            response = wbi_helpers.search_entities(label, language=language, search_type='item', dict_result=True)

            if len(response) == 0:
                raise ValueError(f"No item found for label '{label}'")
            else:
                item_coincidences = []
                for item in response:
                    item_id = item.get('id')
                    full_item = self.wbi.item.get(entity_id=item_id)
                    item_label = full_item.labels.get(language)
                    item_description = str(full_item.descriptions.get(language).value).lower()
            
                    if (item_label and item_label.value == label and 
                        item_description and key_word in item_description):
                        item_coincidences.append(item_id)
                if len(item_coincidences) == 0:
                    raise ValueError(f"No item found for label '{label}' and keyword '{key_word}'")
                elif len(item_coincidences) > 1:
                    raise ValueError(f"Multiple items found for label '{label}'. Disambiguate.")
                else:
                    return item_coincidences[0]
        else:
            return self.find_item_by_label(expression, language)

    def create_property(self, property_schema: PropertySchema, language: str) -> Optional[str]:
        try:
            prop = self.wbi.property.new()
            prop.datatype = property_schema.datatype
            prop.labels.set(language, property_schema.label)
            prop.descriptions.set(language, property_schema.description)
            
            if property_schema.aliases:
                for alias in property_schema.aliases:
                    prop.aliases.set(language, alias)
            
            prop.write(login=self.wbi.login)
            
            property_id = prop.id
            # Update cache
            self.properties_by_label[property_schema.label] = property_id
            return property_id
                
        except Exception as e:
            return None

    def update_property(self, property_schema: PropertySchema, language: str) -> bool:
        try:
            prop = self.wbi.property.get(entity_id=property_schema.id)

            prop.datatype = property_schema.datatype
            prop.labels.set(language, property_schema.label)
            prop.descriptions.set(language, property_schema.description)
            
            prop.aliases.set(language, property_schema.aliases, ActionIfExists.REPLACE_ALL)
            
            prop.write(login=self.wbi.login)

            return True
            
        except Exception as e:
            return False

    def create_item(self, item_schema: ItemSchema, language: str) -> Optional[str]:
        try:
            item = self.wbi.item.new()
            item.labels.set(language, item_schema.label)
            item.descriptions.set(language, item_schema.description)
            
            if item_schema.aliases:
                for alias in item_schema.aliases:
                    item.aliases.set(language, alias)
            
            if item_schema.statements:
                claims_to_add = self._create_claims_from_statements(item_schema.statements, language)
                if claims_to_add:
                    item.add_claims(claims_to_add, ActionIfExists.REPLACE_ALL)
            
            item.write(login=self.wbi.login)
            
            item_id = item.id
            
            # Update cache
            if item_schema.label not in self.items_by_label_and_description:
                self.items_by_label_and_description[item_schema.label] = {}
            self.items_by_label_and_description[item_schema.label][item_schema.description] = item_id
            
            return item_id
                
        except Exception as e:
            return None

    def update_item(self, item_schema: ItemSchema, language: str) -> bool:
        try:
            item = self.wbi.item.get(entity_id=item_schema.id)
            
            item.labels.set(language, item_schema.label)
            item.descriptions.set(language, item_schema.description)
            
            item.aliases.set(language, item_schema.aliases, ActionIfExists.REPLACE_ALL)
            
            if item_schema.statements:
                claims_to_add = self._create_claims_from_statements(item_schema.statements, language)
                if claims_to_add:
                    item.add_claims(claims_to_add, ActionIfExists.REPLACE_ALL)
            
            item.write(login=self.wbi.login)
            
            return True
            
        except Exception as e:
            return False

    def _create_claims_from_statements(self, statements: List[StatementSchema], language: str) -> list:
        claims_to_add = []
        for statement in statements:
            claim = self._create_claim(statement, language)
            if claim:
                claims_to_add.append(claim)
        return claims_to_add

    def _create_claim(self, statement: StatementSchema | ClaimSchema, language: str):
        if not statement.id:
            # Check local cache first
            if statement.label in self.properties_by_label:
                statement_id = self.properties_by_label[statement.label]
            else:
                statement_id = self.find_property_by_label(statement.label, language)
        else:
            statement_id = statement.id
            
        if not statement_id:
            return None
            
        match statement.datatype:
            case 'wikibase-item':
                qualifiers = None
                if hasattr(statement, 'qualifiers') and statement.qualifiers:
                    qualifiers = Qualifiers()
                    for qualifier in statement.qualifiers:
                        qualifiers.add(self._create_claim(qualifier, language))

                references = None
                if hasattr(statement, 'references') and statement.references:
                    references = References()
                    for reference in statement.references:
                        references.add(self._create_claim(reference, language))

                value = statement.value
                if not statement.value.startswith('Q'):
                    # Simple check in local cache first
                    found_in_cache = False
                    if re.match(r'.+ \(.+\)$', statement.value):
                        label = statement.value.split('(')[0].strip()
                        key_word = statement.value.split('(')[1].split(')')[0].strip()
                        if label in self.items_by_label_and_description:
                            for item_id, item_desc in self.items_by_label_and_description[label].items():
                                if key_word in item_desc:
                                    value = item_id
                                    found_in_cache = True
                                    break
                    else:
                        label = statement.value
                        if label in self.items_by_label_and_description:
                            if len(self.items_by_label_and_description[label]) == 1:
                                value = list(self.items_by_label_and_description[label].values())[0]
                                found_in_cache = True

                    if not found_in_cache:
                        value = self.find_item_by_expression(statement.value, language)

                item = Item(prop_nr=statement_id, value=value, qualifiers=qualifiers, references=references)
                return item
            case 'url':
                return URL(prop_nr=statement_id, value=str(statement.value))
            case 'commonsMedia':
                return CommonsMedia(prop_nr=statement_id, value=str(statement.value))
            case 'time':
                return Time(prop_nr=statement_id, time=str(statement.value))
            case 'quantity':
                return Quantity(prop_nr=statement_id, value=str(statement.value))
            case 'external-id':
                return ExternalID(prop_nr=statement_id, value=str(statement.value))
            case _:
                return String(prop_nr=statement_id, value=str(statement.value))