"""Schema synchronization to Wikibase."""

import yaml
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.text import Text
from rich.table import Table
import sys

from ..config.manager import ConfigManager
from .models import SchemaConfig, ItemSchema, PropertySchema
from wbk.backend.interface import BackendStrategy
from wbk.backend.api import ApiBackend

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
        self.language: str = 'es'
        self.backend: BackendStrategy | None = None

    def sync(self, schema_path: str) -> None:
        """Sync schema definitions to Wikibase.
        
        Args:
            schema_path: Path to schema configuration file
        """
        schema_config = self._load_schema_config(schema_path)
        self.language = schema_config.language
        self.backend = ApiBackend(self.config_manager, self.language)
        
        stats = {
            "properties": {"created": 0, "updated": 0, "failed": 0},
            "items": {"created": 0, "updated": 0, "failed": 0}
        }
        
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
                    self._sync_property(prop, stats["properties"])
                    progress.advance(task)
            
            # Sync items
            if schema_config.items:
                task = progress.add_task(
                    f"[blue]Syncing {len(schema_config.items)} items...[/blue]",
                    total=len(schema_config.items)
                )
                for item in schema_config.items:
                    self._sync_item(item, stats["items"])
                    progress.advance(task)
        
        # Create summary table
        table = Table(title="Sync Summary")
        table.add_column("Type", style="cyan")
        table.add_column("Created", style="green")
        table.add_column("Updated", style="yellow")
        table.add_column("Failed", style="red")

        table.add_row(
            "Properties", 
            str(stats["properties"]["created"]), 
            str(stats["properties"]["updated"]),
            str(stats["properties"]["failed"])
        )
        table.add_row(
            "Items", 
            str(stats["items"]["created"]), 
            str(stats["items"]["updated"]),
            str(stats["items"]["failed"])
        )

        console.print(Panel(
            table,
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

    def _sync_property(self, property_schema: PropertySchema, stats: dict) -> None:
        """Sync a single property to Wikibase.
        
        Args:
            property_schema: Property schema definition
            stats: Statistics dictionary
        """
        if not property_schema.id:
            existing_property_id = self.backend.find_property_by_label(
                property_schema.label, 
            )
            
            if existing_property_id:
                property_schema.id = existing_property_id
        
        if property_schema.id:
            if self.backend.update_property(property_schema):
                stats["updated"] += 1
            else:
                stats["failed"] += 1
        else:
            new_id = self.backend.create_property(property_schema)
            if new_id:
                property_schema.id = new_id
                stats["created"] += 1
            else:
                stats["failed"] += 1

    def _sync_item(self, item_schema: ItemSchema, stats: dict) -> None:
        """Sync a single item to Wikibase.
        
        Args:
            item_schema: Item schema definition
            stats: Statistics dictionary
        """
        if not item_schema.id:
            existing_item_id = self.backend.find_item_by_label_and_description(
                item_schema.label,
                item_schema.description,
            )
            
            if existing_item_id:
                item_schema.id = existing_item_id
        
        if item_schema.id:
            if self.backend.update_item(item_schema):
                stats["updated"] += 1
            else:
                stats["failed"] += 1
        else:
            if item_schema.label == "comuna":
                print("debug")
            new_id = self.backend.create_item(item_schema)
            if new_id:
                item_schema.id = new_id
                stats["created"] += 1
            else:
                stats["failed"] += 1
    