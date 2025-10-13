"""Schema synchronization to Wikibase."""

import re
import yaml
from pathlib import Path
from typing import Any
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.text import Text
from wikibaseintegrator.models import Qualifiers, References
from wikibaseintegrator.datatypes import (
    String, ExternalID, Time, Quantity, Item, URL, CommonsMedia
)
import sys
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator import wbi_helpers

from ..config.manager import ConfigManager
from .models import SchemaConfig, StatementSchema, ClaimSchema, ItemSchema, PropertySchema

console = Console(force_terminal=True, width=120)
stderr_console = Console(file=sys.stderr, force_terminal=True, width=120)


class SchemaSyncer:
    """Handles synchronization of properties and items to Wikibase."""
    
    def __init__(self, config_manager: ConfigManager) -> None:
        """Initialize schema syncer.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager
        self.language: str = 'en'
        self.wbi = config_manager.get_wikibase_integrator()
        # cache of properties/items for syncing execution time
        self.properties_by_label: dict[str, str] = {} 
        self.items_by_label_and_description: dict[str, dict[str, str]] = {}

    def items_by_expression(self, expression: str) -> str | None:
        """Get the cache of items by label and description.
        
        Args:
            expression: String to search for, it should has the format "label (substring of description)"
            
        Returns:
            Item ID if found, None otherwise
        """

        if re.match(r'.+ \(.+\)$', expression):
            label = expression.split('(')[0].strip()
            key_word = expression.split('(')[1].split(')')[0].strip()

            items_by_label = self.items_by_label_and_description.get(label, {})
            for item_id, item_description in items_by_label.items():
                if key_word in item_description:
                    return item_id
        else:
            label = expression

            items_by_label = self.items_by_label_and_description.get(label, {})
            if len(items_by_label) == 1:
                return list(items_by_label.values())[0]
            else:
                return None


    
    def sync(self, schema_path: str) -> None:
        """Sync schema definitions to Wikibase.
        
        Args:
            schema_path: Path to schema configuration file
        """
        schema_config = self._load_schema_config(schema_path)
        self.language = schema_config.language
        
        total_tasks = len(schema_config.properties) + len(schema_config.items)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True
        ) as progress:
            
            # Sync properties
            if schema_config.properties:
                task = progress.add_task(
                    f"[blue]Syncing {len(schema_config.properties)} properties...[/blue]",
                    total=len(schema_config.properties)
                )
                for prop in schema_config.properties:
                    self._sync_property(prop)
                    progress.advance(task)
            
            # Sync items
            if schema_config.items:
                task = progress.add_task(
                    f"[blue]Syncing {len(schema_config.items)} items...[/blue]",
                    total=len(schema_config.items)
                )
                for item in schema_config.items:
                    self._sync_item(item)
                    progress.advance(task)
        
        console.print(Panel(
            Text("✓ Schema synchronization completed!", style="green bold"),
            title="Success",
            border_style="green"
        ))
    
    def _load_schema_config(self, schema_path: str) -> SchemaConfig:
        """Load schema configuration from YAML file.
        
        Args:
            schema_path: Path to schema configuration file
            
        Returns:
            Loaded schema configuration
        """
        schema_file = Path(schema_path)
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_data = yaml.safe_load(f)
        
        return SchemaConfig(**schema_data)


    def _find_property_by_label(self, label: str) -> str | None:
        """Find a property by exact label match.
        
        Args:
            label: Property label to search for
            
        Returns:
            Property ID if found, None otherwise
        """
        try:
            # Use SPARQL query to find properties by label
            query = f"""
            SELECT ?propertyID WHERE {{
            ?property a wikibase:Property ;
                        rdfs:label "{label}"@{self.language} .

            BIND(STRAFTER(STR(?property), "/entity/") AS ?propertyID)
            }}
            """
            
            results = wbi_helpers.execute_sparql_query(query)
            
            if results and 'results' in results and 'bindings' in results['results']:
                return results['results']['bindings'][0]['propertyID']['value']
            
            return None
        
        except Exception as e:
            console.print(f"    [yellow]SPARQL search failed, falling back to search_entities: {e}[/yellow]")
            # Fallback to search_entities if SPARQL fails
            properties = wbi_helpers.search_entities(label, language=self.language, search_type='property', dict_result=True)

            for prop in properties:
                console.print(f"[green]Checking property in cache: {prop['label']}[/green]")
                prop_id = prop.get('id', 'Unknown')
                try:
                    full_prop = self.wbi.property.get(entity_id=prop_id)
                    prop_label = full_prop.labels.get(self.language)
                    
                    if prop_label and prop_label.value == label:
                        return prop_id
                except Exception:
                    continue
            
            return None
    
    
    def _find_property_by_label_and_description(self, label: str, description: str) -> str | None:
        """Find a property by exact label and description match.
        
        Args:
            label: Property label to search for
            description: Property description to search for
            
        Returns:
            Property ID if found, None otherwise
        """
        try:
            # Use SPARQL query to find properties by label and description
            query = f"""
            SELECT ?property WHERE {{
                ?property rdf:type wikibase:Property .
                ?property rdfs:label "{label}"@{self.language} .
                ?property schema:description "{description}"@{self.language} .
            }}
            """
            
            results = wbi_helpers.execute_sparql_query(query)

            
            if results and 'results' in results and 'bindings' in results['results']:
                bindings = results['results']['bindings']
                if bindings:
                    # Extract property ID from the URI
                    property_uri = bindings[0]['property']['value']
                    property_id = property_uri.split('/')[-1]  # Extract P123 from full URI
                    return property_id
            
            return None
            
        except Exception as e:
            console.print(f"    [yellow]SPARQL search failed, falling back to search_entities: {e}[/yellow]")
            # Fallback to search_entities if SPARQL fails
            properties = wbi_helpers.search_entities(label, language=self.language, search_type='property', dict_result=True)
            
            for prop in properties:
                prop_id = prop.get('id', 'Unknown')
                try:
                    full_prop = self.wbi.property.get(entity_id=prop_id)
                    prop_label = full_prop.labels.get(self.language)
                    prop_description = full_prop.descriptions.get(self.language)
                    
                    if (prop_label and prop_label.value == label and 
                        prop_description and prop_description.value == description):
                        return prop_id
                except Exception:
                    continue
            
            return None

    def _sync_property(self, property_schema) -> None:
        """Sync a single property to Wikibase.
        
        Args:
            property_schema: Property schema definition
        """
        console.print(f"  [cyan]Syncing property: {property_schema.label}[/cyan]")

        # Try to find existing property by label and description
        if not property_schema.id:
            existing_property_id = self._find_property_by_label_and_description(
                property_schema.label, 
                property_schema.description
            )
            
            if existing_property_id:
                property_schema.id = existing_property_id
                console.print(f"    [green]Found existing property: {existing_property_id}[/green]")
            else:
                console.print(f"    [yellow]No existing property found with label '{property_schema.label}' and description '{property_schema.description}'[/yellow]")
        
        if property_schema.id:
            console.print(f"    [yellow]Updating existing property {property_schema.id}[/yellow]")
            self._update_property(property_schema)
        else:
            console.print(f"    [yellow]Creating new property: {property_schema.label}[/yellow]")
            property_schema.id = self._create_property(property_schema)

        self.properties_by_label[property_schema.label] = property_schema.id



    def _find_item_by_label(self, label: str) -> str | None:
        """Find an item by exact label match.
        
        Args:
            label: Item label to search for
            
        Returns:
            Item ID if found, None otherwise
        """
        try:
            # Use SPARQL query to find items by label
            query = f"""
            SELECT ?itemID WHERE {{
                ?item rdfs:label "{label}"@{self.language} .
                BIND(STRAFTER(STR(?item), "/entity/") AS ?itemID)
            }}
            """

            response = wbi_helpers.execute_sparql_query(query)['results']['bindings']

            if len(response) == 0:
                raise ValueError(f"No item found for label '{label}'")
            elif len(response) > 1:
                raise ValueError(f"Multiple items found for label '{label}'")
            else:
                return response[0]['itemID']['value']

        except Exception as e:
            console.print(f"    [yellow]SPARQL search failed, falling back to search_entities: {e}[/yellow]")
            return None

    def _find_item_by_label_and_description(self, label: str, description: str) -> str | None:
        """Find an item by exact label and description match.
        
        Args:
            label: Item label to search for
            description: Item description to search for
            
        Returns:
            Item ID if found, None otherwise
        """
        try:
            # Use SPARQL query to find items by label and description
            query = f"""
            SELECT ?itemID WHERE {{
                ?item rdfs:label "{label}"@{self.language} .
                ?item schema:description "{description}"@{self.language} .
                BIND(STRAFTER(STR(?item), "/entity/") AS ?itemID)
            }}
            """
    
            from wikibaseintegrator import wbi_helpers
            response = wbi_helpers.execute_sparql_query(query)['results']['bindings']

            if len(response) == 0:
                raise ValueError(f"No item found for label '{label}' and description '{description}'")
            elif len(response) > 1:
                raise ValueError(f"Multiple items found for label '{label}' and description '{description}'")
            else:
                return response[0]['itemID']['value']
            
            return None
    
        except Exception as e:
            console.print(f"    [yellow]SPARQL search failed, falling back to search_entities: {e}[/yellow]")
            # Fallback to search_entities if SPARQL fails
            from wikibaseintegrator import wbi_helpers
            items = wbi_helpers.search_entities(label, language=self.language, search_type='item', dict_result=True)
            
            for item in items:
                item_id = item.get('id', 'Unknown')
                try:
                    full_item = self.wbi.item.get(entity_id=item_id)
                    item_label = full_item.labels.get(self.language)
                    item_description = full_item.descriptions.get(self.language)
            
                    if (item_label and item_label.value == label and 
                        item_description and item_description.value == description):
                        return item_id
                except Exception:
                    continue
            
            return None


    def _find_item_by_expression(self, expression:str) -> str | None:
        """Find an item by label and description match.
        
        Args:
            expression: String to search for, it should has the format "label (substring of description)" or just "label"
            
        Returns:
            Item ID if found, None otherwise
        """
        if re.match(r'.+ \(.+\)$', expression):
            label = expression.split('(')[0].strip()
            description = expression.split('(')[1].split(')')[0].strip()
            try:
                # Use SPARQL query to find items by label and description substring
                query = f"""
                SELECT ?itemID WHERE {{
                    ?item rdfs:label "{label}"@{self.language} .
                    ?item schema:description ?desc .
                    FILTER(CONTAINS(?desc, "{description}"))

                    BIND(STRAFTER(STR(?item), "/entity/") AS ?itemID)
                }}
                """

                response = wbi_helpers.execute_sparql_query(query)['results']['bindings']

                if len(response) == 0:
                    raise ValueError(f"No item found for label '{label}' and description substring '{description}'")
                elif len(response) > 1:
                    raise ValueError(f"Multiple items found for label '{label}' and description substring '{description}'")
                else:
                    return response[0]['itemID']['value']
            except Exception as e:
                console.print(f"    [yellow]SPARQL search failed, falling back to search_entities: {e}[/yellow]")
                return None
        else:
            try:
                return self._find_item_by_label(expression)
            except Exception as e:
                console.print(f"    [yellow]SPARQL search failed, falling back to search_entities: {e}[/yellow]")
                return None



    def _sync_item(self, item_schema: ItemSchema) -> None:
        """Sync a single item to Wikibase.
        
        Args:
            item_schema: Item schema definition
        """
        console.print(f"  [cyan]Syncing item: {item_schema.label}[/cyan]")

        if not item_schema.id:
            existing_item_id = self._find_item_by_label_and_description(
                item_schema.label,
                item_schema.description
            )
            
            if existing_item_id:
                item_schema.id = existing_item_id
                console.print(f"    [green]Found existing item: {existing_item_id}[/green]")
            else:
                console.print(f"    [yellow]No existing item found with label '{item_schema.label}' and description '{item_schema.description}'[/yellow]")
        
        if item_schema.id:
            console.print(f"    [yellow]Updating existing item {item_schema.id}[/yellow]")
            self._update_item(item_schema)
        else:
            console.print(f"    [yellow]Creating new item[/yellow]")
            item_schema.id = self._create_item(item_schema)

        
        if item_schema.label not in self.items_by_label_and_description:
            self.items_by_label_and_description[item_schema.label] = {}
        self.items_by_label_and_description[item_schema.label][item_schema.description] = item_schema.id

    
    def _create_property(self, property_schema: PropertySchema) -> str | None:
        """Create a new property in Wikibase.
        
        Args:
            property_schema: Property schema definition
            
        Returns:
            Property ID if successful, None otherwise
        """
        try:
            # Create new property using Wikibase Integrator
            prop = self.wbi.property.new()
            prop.datatype = property_schema.datatype
            prop.labels.set(self.language, property_schema.label)
            prop.descriptions.set(self.language, property_schema.description)
            
            # Add aliases if provided
            if property_schema.aliases:
                for alias in property_schema.aliases:
                    prop.aliases.set(self.language, alias)
            
            # Write the property to Wikibase
            prop.write(login=self.wbi.login)
            
            property_id = prop.id
            console.print(f"    [green]✓ Created property {property_id}[/green]")
            return property_id
                
        except Exception as e:
            stderr_console.print(f"    [red]✗ Error creating property: {e}[/red]")
            return None
    
    def _update_property(self, property_schema: PropertySchema) -> bool:
        """Update an existing property in Wikibase.
        
        Args:
            property_schema: Property schema definition
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get existing property using Wikibase Integrator
            prop = self.wbi.property.get(entity_id=property_schema.id)
            
            # Update labels, descriptions, and aliases
            prop.labels.set(self.language, property_schema.label)
            prop.descriptions.set(self.language, property_schema.description)
            
            # Replace all aliases for English
            prop.aliases.set(self.language, property_schema.aliases, ActionIfExists.REPLACE_ALL)
            
            # Write the updated property
            prop.write(login=self.wbi.login)
            
            console.print(f"    [green]✓ Updated property {property_schema.id}[/green]")
            return True
            
        except Exception as e:
            stderr_console.print(f"    [red]✗ Error updating property: {e}[/red]")
            return False
    
    def _create_item(self, item_schema: ItemSchema) -> str | None:
        """Create a new item in Wikibase.
        
        Args:
            item_schema: Item schema definition
            
        Returns:
            Item ID if successful, None otherwise
        """
        try:
            # Create new item using Wikibase Integrator
            item = self.wbi.item.new()
            item.labels.set(self.language, item_schema.label)
            item.descriptions.set(self.language, item_schema.description)
            
            # Add aliases if provided
            if item_schema.aliases:
                for alias in item_schema.aliases:
                    item.aliases.set(self.language, alias)
            
            # Update statements if provided
            if item_schema.statements:
                # Get all claims to add
                claims_to_add = self._create_claims_from_statements(item_schema.statements)
                if claims_to_add:
                    # Replace all claims using ActionIfExists.REPLACE_ALL
                    item.add_claims(claims_to_add, ActionIfExists.REPLACE_ALL)
            
            # Write the item to Wikibase
            item.write(login=self.wbi.login)
            
            item_id = item.id
            console.print(f"    [green]✓ Created item {item_id}[/green]")
            return item_id
                
        except Exception as e:
            stderr_console.print(f"    [red]✗ Error creating item: {e}[/red]")
            return None
    
    def _update_item(self, item_schema: ItemSchema) -> bool:
        """Update an existing item in Wikibase.
        
        Args:
            item_schema: Item schema definition
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get existing item using Wikibase Integrator
            item = self.wbi.item.get(entity_id=item_schema.id)
            
            # Update labels, descriptions, and aliases
            item.labels.set(self.language, item_schema.label)
            item.descriptions.set(self.language, item_schema.description)
            
            # Replace all aliases for English
            item.aliases.set(self.language, item_schema.aliases, ActionIfExists.REPLACE_ALL)
            
            # Update statements if provided
            if item_schema.statements:
                # Get all claims to add
                claims_to_add = self._create_claims_from_statements(item_schema.statements)
                if claims_to_add:
                    # item.add_claims(claims_to_add, ActionIfExists.FORCE_APPEND)
                    
                    item.add_claims(claims_to_add, ActionIfExists.REPLACE_ALL)
            
            # Write the updated item
            item.write(login=self.wbi.login)
            
            console.print(f"    [green]✓ Updated item {item_schema.id}[/green]")
            return True
            
        except Exception as e:
            stderr_console.print(f"    [red]✗ Error updating item: {e}[/red]")
            return False
    
    def _create_claims_from_statements(self, statements: list[StatementSchema]) -> list:
        """Create claim objects from statements configuration.
        
        Args:
            statements: Dictionary of property configurations
            
        Returns:
            List of Claim objects
        """
        
        claims_to_add = []

        for statement in statements:
            claim = self._create_claim(statement)
            if claim:
                claims_to_add.append(claim)

        return claims_to_add
    
    def _create_claim(self, statement: StatementSchema | ClaimSchema):
        """Create a claim object based on value and datatype.
        
        Args:
            statement: Statement schema definition
            
        Returns:
            Claim object or None if creation fails
            
        """
        if not statement.id:
            # First check local cache for properties created in this sync session
            if statement.label in self.properties_by_label:
                statement_id = self.properties_by_label[statement.label]
            else:
                # Fallback to SPARQL search for existing properties
                statement_id = self._find_property_by_label(statement.label)
        else:
            statement_id = statement.id  # Use the provided ID
            
        if not statement_id:
            return None
            
        try:
            match statement.datatype:
                case 'wikibase-item':
                    qualifiers = None
                    if statement.qualifiers:
                        qualifiers = Qualifiers()
                        for qualifier in statement.qualifiers:
                            qualifiers.add(self._create_claim(qualifier))

                    references = None
                    if statement.references:
                        references = References()
                        for reference in statement.references:
                            references.add(self._create_claim(reference))

                    if not statement.value.startswith('Q'):
                        value = self.items_by_expression(statement.value)
                        if not value:
                            value = self._find_item_by_expression(statement.value)

                    item = Item(prop_nr=statement_id, value=value, qualifiers=qualifiers, references=references)
                    # console.print(f"[green]Created item: {item}[/green]")
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
        except Exception as e:
            console.print(f"[yellow]Warning: Could not create claim for value '{statement.value}' with datatype '{statement.datatype}': {e}[/yellow]")
            return None
    