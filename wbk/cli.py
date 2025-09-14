"""CLI interface for Wikibase Bulk Kit."""

import click
from pathlib import Path
from rich.console import Console
import sys

from wbk.config.manager import ConfigManager
from wbk.schema.sync import SchemaSyncer

console = Console()
stderr_console = Console(file=sys.stderr)


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """Wikibase Bulk Kit - A modular framework for bulk uploading CSV datasets to Wikibase"""
    pass


@cli.command()
@click.option(
    '--config', '-c', 
    'config_path', 
    type=click.Path(exists=True, path_type=Path),
    default='configs/project.yml', 
    help='Path to project config'
)
@click.option(
    '--schema', '-s', 
    'schema_path', 
    type=click.Path(exists=True, path_type=Path),
    default='configs/schema.yml', 
    help='Path to schema config'
)
def schema(config_path: Path, schema_path: Path) -> None:
    """Sync properties and items into Wikibase from schema.yml."""
    console.print("[blue]Starting schema synchronization to Wikibase...[/blue]")
    
    try:
        config_manager = ConfigManager(str(config_path))
        schema_syncer = SchemaSyncer(config_manager)
        schema_syncer.sync(str(schema_path))
        console.print("[green]✓ Schema sync completed successfully![/green]")
    except Exception as e:
        stderr_console.print(f"[red]✗ Schema sync failed: {e}[/red]")
        raise click.Abort()


if __name__ == "__main__":
    cli()