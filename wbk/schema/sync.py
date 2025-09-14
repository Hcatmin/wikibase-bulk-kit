"""Schema synchronization to Wikibase."""

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

from ..config.manager import ConfigManager
from .models import SchemaConfig

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
    
    def _sync_property(self, property_schema) -> None:
        """Sync a single property to Wikibase.
        
        Args:
            property_schema: Property schema definition
        """
        console.print(f"  [cyan]Syncing property: {property_schema.label}[/cyan]")
        
        if property_schema.id:
            console.print(f"    [yellow]Updating existing property {property_schema.id}[/yellow]")
            self._update_property(property_schema)
        else:
            console.print(f"    [yellow]Creating new property: {property_schema.datatype}[/yellow]")
            self._create_property(property_schema)
    
    def _sync_item(self, item_schema) -> None:
        """Sync a single item to Wikibase.
        
        Args:
            item_schema: Item schema definition
        """
        console.print(f"  [cyan]Syncing item: {item_schema.label}[/cyan]")
        
        if item_schema.id:
            console.print(f"    [yellow]Updating existing item {item_schema.id}[/yellow]")
            self._update_item(item_schema)
        else:
            console.print(f"    [yellow]Creating new item[/yellow]")
            new_id = self._create_item(item_schema)
            if new_id:
                item_schema.id = new_id  # Update schema with new ID
    
    def _create_property(self, property_schema) -> str | None:
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
    
    def _update_property(self, property_schema) -> bool:
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
    
    def _create_item(self, item_schema) -> str | None:
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
    
    def _update_item(self, item_schema) -> bool:
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
                    # Replace all claims using ActionIfExists.REPLACE_ALL
                    item.add_claims(claims_to_add, ActionIfExists.REPLACE_ALL)
            
            # Write the updated item
            item.write(login=self.wbi.login)
            
            console.print(f"    [green]✓ Updated item {item_schema.id}[/green]")
            return True
            
        except Exception as e:
            stderr_console.print(f"    [red]✗ Error updating item: {e}[/red]")
            return False
    
    def _create_claims_from_statements(self, statements: dict[str, Any]) -> list:
        """Create claim objects from statements configuration.
        
        Args:
            statements: Dictionary of property configurations
            
        Returns:
            List of Claim objects
        """
        
        claims_to_add = []
        
        for prop_id, statement_config in statements.items():
            if prop_id.startswith('P'):
                # Handle both simple values and complex statement configurations
                if isinstance(statement_config, dict):
                    # Complex statement with value, qualifiers, references
                    value = statement_config.get('value')
                    datatype = statement_config.get('datatype', 'string')
                    qualifiers = statement_config.get('qualifiers', {})
                    references = statement_config.get('references', [])
                    unit = statement_config.get('unit')
                else:
                    # Simple value (backward compatibility)
                    value = statement_config
                    datatype = 'string'
                    qualifiers = {}
                    references = []
                    unit = None
                
                # Create qualifiers object if needed
                claim_qualifiers = None
                if qualifiers:
                    claim_qualifiers = Qualifiers()
                    for qual_prop, qual_config in qualifiers.items():
                        if isinstance(qual_config, dict):
                            qual_value = qual_config.get('value')
                            qual_datatype = qual_config.get('datatype', 'string')
                            qual_unit = qual_config.get('unit')
                        else:
                            qual_value = qual_config
                            qual_datatype = 'string'
                            qual_unit = None
                        
                        qual_claim = self._create_claim(qual_value, qual_datatype, qual_unit, qual_prop)
                        if qual_claim:
                            claim_qualifiers.add(qual_claim)
                
                # Create references object if needed
                claim_references = None
                if references:
                    claim_references = References()
                    for ref_config in references:
                        if isinstance(ref_config, dict):
                            for ref_prop, ref_value_config in ref_config.items():
                                if isinstance(ref_value_config, dict):
                                    ref_value = ref_value_config.get('value')
                                    ref_datatype = ref_value_config.get('datatype', 'string')
                                    ref_unit = ref_value_config.get('unit')
                                else:
                                    ref_value = ref_value_config
                                    ref_datatype = 'string'
                                    ref_unit = None
                                
                                ref_claim = self._create_claim(ref_value, ref_datatype, ref_unit, ref_prop)
                                if ref_claim:
                                    claim_references.add(ref_claim)
                
                # Create the main claim with qualifiers and references
                claim = self._create_claim(value, datatype, unit, prop_id)
                if claim:
                    # Attach qualifiers and references to the claim
                    if claim_qualifiers:
                        claim.qualifiers = claim_qualifiers
                    if claim_references:
                        claim.references = claim_references
                    
                    claims_to_add.append(claim)
        
        return claims_to_add
    
    def _create_claim(self, value: Any, datatype: str, unit: str = None, prop_nr: str = None):
        """Create a claim object based on value and datatype.
        
        Args:
            value: The claim value
            datatype: The datatype (string, wikibase-item, quantity, time, url, etc.)
            unit: Unit for quantity claims
            prop_nr: Property number (P123)
            
        Returns:
            Claim object or None if creation fails
        """
        try:
            match datatype:
                case 'wikibase-item':
                    return Item(prop_nr=prop_nr, value=value)
                case 'quantity':
                    if unit:
                        return Quantity(prop_nr=prop_nr, value=value, unit=unit)
                    else:
                        return Quantity(prop_nr=prop_nr, value=value)
                case 'time':
                    return Time(prop_nr=prop_nr, value=value)
                case 'url':
                    return URL(prop_nr=prop_nr, value=value)
                case 'string':
                    return String(prop_nr=prop_nr, value=str(value))
                case 'external-id':
                    return ExternalID(prop_nr=prop_nr, value=str(value))
                case 'commonsMedia':
                    return CommonsMedia(prop_nr=prop_nr, value=str(value))
                case _:
                    # Default to string
                    return String(prop_nr=prop_nr, value=str(value))
        except Exception as e:
            console.print(f"[yellow]Warning: Could not create claim for value '{value}' with datatype '{datatype}': {e}[/yellow]")
            return None
    