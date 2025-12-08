"""CLI interface for Wikibase Bulk Kit."""

import click
from pathlib import Path
from rich.console import Console
import sys

from wbk.mapping.processor import MappingProcessor
from wbk.schema.sync import SchemaSyncer
from RaiseWikibase.raiser import building_indexing, update_links

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
    '--path', '-p', 
    'schema_path', 
    type=click.Path(exists=True, path_type=Path),
    help='Path to schema config'
)
def schema(config_path: Path, schema_path: Path) -> None:
    """Sync properties and items into Wikibase from schema.yml."""
    console.print("[blue]Starting schema synchronization to Wikibase...[/blue]")
    
    try:
        schema_syncer = SchemaSyncer()
        schema_syncer.sync(str(schema_path))
        console.print("[green]✓ Schema sync completed successfully![/green]")
    except Exception as e:
        stderr_console.print(f"[red]✗ Schema sync failed: {e}[/red]")
        raise click.Abort()


@cli.command(name="mapping")
@click.option(
    '--path', '-p', 
    'mapping_path', 
    type=click.Path(exists=True, path_type=Path),
    help='Path to mapping config'
)
def mapping(mapping_path: Path) -> None:
    """Process CSV files using the experimental pipeline implementation."""
    console.print("[blue]Starting mapping process...[/blue]")

    try:
        mapping_processor = MappingProcessor()
        console.print(f"[blue] Processing mapping config from {mapping_path}[/blue]")
        mapping_processor.process(str(mapping_path))
        console.print("[green]✓ Mapping process completed successfully![/green]")
    except Exception as e:
        stderr_console.print(f"[red]✗ Mapping process failed: {e}[/red]")
        raise click.Abort()


@cli.command(name="indexing")
def indexing() -> None:
    """Build indexing tables for Wikibase."""
    console.print("[blue]Building indexing tables for Wikibase...[/blue]")
    try:
        building_indexing()
        console.print("[green]✓ Indexing tables built successfully![/green]")
    except Exception as e:
        stderr_console.print(f"[red]✗ Indexing tables build failed: {e}[/red]")
        raise click.Abort()


@cli.command(name="links")
def links() -> None:
    """Update links tables for Wikibase."""
    console.print("[blue]Updating links tables for Wikibase...[/blue]")
    try:
        update_links()
        console.print("[green]✓ Links tables updated successfully![/green]")
    except Exception as e:
        stderr_console.print(f"[red]✗ Links tables update failed: {e}[/red]")
        raise click.Abort()
        
if __name__ == "__main__":
    cli()
