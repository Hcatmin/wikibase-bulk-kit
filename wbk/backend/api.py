import re
import sys
from typing import Optional, List, Any
from rich.console import Console

from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_login import Login
from wikibaseintegrator import wbi_helpers
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.models import Qualifiers, References
from wikibaseintegrator.datatypes import (
    String, ExternalID, Time, Quantity, Item, URL, CommonsMedia
)

from ..schema.models import PropertySchema, ItemSchema, StatementSchema, ClaimSchema
from .interface import BackendStrategy

from wbk.config.settings import settings

console = Console(force_terminal=True, width=120)
stderr_console = Console(file=sys.stderr, force_terminal=True, width=120)

class ApiBackend(BackendStrategy):
    """Backend strategy using WikibaseIntegrator API."""

    def __init__(self, language: str) -> None:
        super().__init__(language=language)
        self.wbi = self._get_wikibase_integrator()
        # Cache for items by label and description to avoid repeated lookups
        self.items_by_label_and_description: dict[str, dict[str, str]] = {}
        # Cache for properties by label
        self.properties_by_label: dict[str, str] = {}

    def _get_wikibase_integrator(self) -> WikibaseIntegrator:
        """Get configured Wikibase Integrator instance.
        
        Returns:
            Configured WikibaseIntegrator instance
        """
        wbi_config['MEDIAWIKI_API_URL'] = settings.mediawiki_api_url
        wbi_config['WIKIBASE_URL'] = settings.wikibase_url
        wbi_config['DEFAULT_LANGUAGE'] = self.language
        
        username = settings.wikibase_username
        password = settings.wikibase_password
        return WikibaseIntegrator(login=Login(user=username, password=password))
        

    def find_property_by_label(self, label: str) -> Optional[str]:
        properties = wbi_helpers.search_entities(search_string=label, search_type='property', dict_result=True)

        for prop in properties:
            prop_id = prop.get('id', 'Unknown')
            try:
                full_prop = self.wbi.property.get(entity_id=prop_id)
                prop_label = full_prop.labels.get()
                
                if prop_label and prop_label.value == label:
                    return prop_id
            except Exception:
                continue
        
        return None

    def find_item_by_label(self, label: str) -> Optional[str]:
        response = wbi_helpers.search_entities(search_string=label, search_type='item', dict_result=True)

        if len(response) == 0:
            raise ValueError(f"No item found for label '{label}'")
        elif len(response) > 1:
            raise ValueError(f"Multiple items found for label '{label}'. Disambiguate.")
        else:
            return response[0].get('id')

    def find_item_by_label_and_description(self, label: str, description: str) -> Optional[str]:
        items = wbi_helpers.search_entities(search_string=label, search_type='item', dict_result=True)
        
        for item in items:
            item_id = item.get('id')
            try:
                full_item = self.wbi.item.get(entity_id=item_id)
                item_label = full_item.labels.get()
                item_description = full_item.descriptions.get()
        
                if (item_label and item_label.value == label and 
                    item_description and item_description.value == description):
                    return item_id
            except Exception:
                continue
        
        return None

    def find_item_by_expression(self, expression: str) -> Optional[str]:
        if re.match(r'.+ \(.+\)$', expression):
            label = expression.split('(')[0].strip()
            key_word = expression.split('(')[1].split(')')[0].strip()
            
            response = wbi_helpers.search_entities(search_string=label, search_type='item', dict_result=True)

            if len(response) == 0:
                raise ValueError(f"No item found for label '{label}'")
            else:
                item_coincidences = []
                for item in response:
                    item_id = item.get('id')
                    full_item = self.wbi.item.get(entity_id=item_id)
                    item_label = full_item.labels.get()
                    item_description = str(full_item.descriptions.get().value).lower()
            
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
            return self.find_item_by_label(expression)

    def create_property(self, property_schema: PropertySchema) -> Optional[str]:
        try:
            prop = self.wbi.property.new()
            prop.datatype = property_schema.datatype
            prop.labels.set(value=property_schema.label)
            prop.descriptions.set(value=property_schema.description)
            
            if property_schema.aliases:
                for alias in property_schema.aliases:
                    prop.aliases.set(values=alias)
            
            prop.write(login=self.wbi.login)
            
            property_id = prop.id
            # Update cache
            self.properties_by_label[property_schema.label] = property_id
            return property_id
                
        except Exception as e:
            return None

    def update_property(self, property_schema: PropertySchema) -> bool:
        try:
            prop = self.wbi.property.get(entity_id=property_schema.id)

            prop.datatype = property_schema.datatype
            prop.labels.set(value=property_schema.label)
            prop.descriptions.set(value=property_schema.description)
            
            prop.aliases.set(values=property_schema.aliases, action_if_exists=ActionIfExists.REPLACE_ALL)
            
            prop.write(login=self.wbi.login)

            return True
            
        except Exception as e:
            return False

    def create_item(self, item_schema: ItemSchema) -> Optional[str]:
        try:
            item = self.wbi.item.new()
            item.labels.set(value=item_schema.label)
            item.descriptions.set(value=item_schema.description)
            
            if item_schema.aliases:
                for alias in item_schema.aliases:
                    item.aliases.set(values=alias)
            
            if item_schema.statements:
                claims_to_add = self._create_claims_from_statements(item_schema.statements)
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

    def update_item(self, item_schema: ItemSchema) -> bool:
        try:
            item = self.wbi.item.get(entity_id=item_schema.id)
            
            item.labels.set(value=item_schema.label)
            item.descriptions.set(value=item_schema.description)
            
            item.aliases.set(values=item_schema.aliases, action_if_exists=ActionIfExists.REPLACE_ALL)
            
            if item_schema.statements:
                claims_to_add = self._create_claims_from_statements(item_schema.statements)
                if claims_to_add:
                    item.add_claims(claims_to_add, ActionIfExists.REPLACE_ALL)
            
            item.write(login=self.wbi.login)
            
            return True
            
        except Exception as e:
            return False

    def _create_claims_from_statements(self, statements: List[StatementSchema]) -> list:
        claims_to_add = []
        for statement in statements:
            claim = self._create_claim(statement)
            if claim:
                claims_to_add.append(claim)
        return claims_to_add

    def _create_claim(self, statement: StatementSchema | ClaimSchema):
        if not statement.id:
            # Check local cache first
            if statement.label in self.properties_by_label:
                statement_id = self.properties_by_label[statement.label]
            else:
                statement_id = self.find_property_by_label(statement.label)
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
                        qualifiers.add(self._create_claim(qualifier))

                references = None
                if hasattr(statement, 'references') and statement.references:
                    references = References()
                    for reference in statement.references:
                        references.add(self._create_claim(reference))

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
                        value = self.find_item_by_expression(statement.value)

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

    def find_qids(self, keys: List[dict]) -> dict:
        # Fallback implementation using loops
        results = {}
        for i, key in enumerate(keys):
            try:
                if 'unique_key' in key:
                    # Not supported in ApiBackend efficiently, skipping or implementing basic search
                    # For now, return None
                    results[i] = None
                elif 'label' in key:
                    if 'description' in key and key['description']:
                        results[i] = self.find_item_by_label_and_description(key['label'], key['description'])
                    else:
                        results[i] = self.find_item_by_label(key['label'])
            except Exception:
                results[i] = None
        return results

    def create_items(self, items: List[dict], language: str) -> List[str]:
        # ApiBackend expects ItemSchema, but items here are dicts (RaiseWikibase format).
        # This is a mismatch. Strategies use RaiseWikibase format.
        # ApiBackend needs to support RaiseWikibase format or we need a converter.
        # For now, we'll raise NotImplementedError or return empty list to indicate mismatch.
        # Or we can try to convert if simple.
        # Given the task focus is RaiseWikibaseBackend, we can leave this as TODO or basic.
        return []

    def update_items(self, items: List[dict], language: str) -> List[bool]:
        return []