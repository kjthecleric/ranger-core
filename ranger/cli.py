"""Ranger CLI — command-line interface for pipeline execution and management.

Usage::

    ranger run --config pipeline.yaml
    ranger validate --config pipeline.yaml
    ranger schema show --pipeline orders_daily
    ranger schema history --pipeline orders_daily
    ranger discover --config source_config.yaml
    ranger plugins list
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="ranger",
    help="Ranger — Reliable Data Extraction & Loading Platform",
    no_args_is_help=True,
)
console = Console()


@app.command()
def run(
    config: str = typer.Option(..., "--config", "-c", help="Path to pipeline YAML config"),
    metadata_db: str = typer.Option("ranger_meta.duckdb", "--metadata-db", help="Path to DuckDB metadata file"),
) -> None:
    """Execute a pipeline run."""
    from ranger.core.pipeline import Pipeline

    console.print(f"[bold yellow]🤠 Ranger[/] Loading pipeline from [cyan]{config}[/]...")

    pipeline = Pipeline.from_yaml(config, metadata_db=metadata_db)
    result = pipeline.execute()

    if result.status.value == "success":
        console.print(f"\n[bold green]✓ Pipeline completed successfully[/]")
    else:
        console.print(f"\n[bold red]✗ Pipeline failed: {result.error_message}[/]")

    table = Table(title="Run Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="yellow")
    table.add_row("Run ID", result.run_id)
    table.add_row("Status", result.status.value)
    table.add_row("Records Read", str(result.records_read))
    table.add_row("Records Written", str(result.records_written))
    table.add_row("Records Failed", str(result.records_failed))
    table.add_row("Bytes Processed", str(result.bytes_processed))
    table.add_row("Late Records", str(result.late_records_count))
    table.add_row("Schema Drift", str(result.schema_drift_detected))
    if result.duration_seconds is not None:
        table.add_row("Duration", f"{result.duration_seconds:.2f}s")
    console.print(table)


@app.command()
def validate(
    config: str = typer.Option(..., "--config", "-c", help="Path to pipeline YAML config"),
) -> None:
    """Validate a pipeline config without executing."""
    from ranger.core.config import validate_config as _validate

    console.print(f"[bold yellow]🤠 Ranger[/] Validating [cyan]{config}[/]...")

    errors = _validate(config)
    if not errors:
        console.print("[bold green]✓ Configuration is valid[/]")
    else:
        console.print("[bold red]✗ Validation errors:[/]")
        for err in errors:
            console.print(f"  • {err}")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Schema sub-commands
# ---------------------------------------------------------------------------

schema_app = typer.Typer(name="schema", help="Schema management commands", no_args_is_help=True)
app.add_typer(schema_app)


@schema_app.command("show")
def schema_show(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help="Pipeline name"),
    source: str = typer.Option("", "--source", "-s", help="Source name (optional)"),
    metadata_db: str = typer.Option("ranger_meta.duckdb", "--metadata-db", help="Path to DuckDB metadata file"),
) -> None:
    """Display the active schema for a pipeline."""
    from ranger.schema.duckdb_registry import DuckDBSchemaRegistry

    reg = DuckDBSchemaRegistry(db_path=metadata_db)
    source_name = source or pipeline

    version = reg.get_active_schema_version(pipeline, source_name)
    if not version:
        console.print(f"[yellow]No schema found for {pipeline}/{source_name}[/]")
        return

    console.print(f"\n[bold yellow]Schema v{version.version_number}[/] for [cyan]{pipeline}/{source_name}[/]")
    console.print(f"  ID: {version.schema_id}")
    console.print(f"  Fingerprint: {version.fingerprint}")
    console.print(f"  Discovered: {version.discovered_at}")

    table = Table(title="Columns")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="yellow")
    table.add_column("Nullable", style="green")
    for col in version.schema_definition.columns:
        table.add_row(col.name, col.type.value, "Yes" if col.nullable else "No")
    console.print(table)

    reg.close()


@schema_app.command("history")
def schema_history(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help="Pipeline name"),
    source: str = typer.Option("", "--source", "-s", help="Source name (optional)"),
    metadata_db: str = typer.Option("ranger_meta.duckdb", "--metadata-db", help="Path to DuckDB metadata file"),
) -> None:
    """Show schema version history for a pipeline."""
    from ranger.schema.duckdb_registry import DuckDBSchemaRegistry

    reg = DuckDBSchemaRegistry(db_path=metadata_db)
    source_name = source or pipeline

    versions = reg.get_schema_history(pipeline, source_name)
    if not versions:
        console.print(f"[yellow]No schema history for {pipeline}/{source_name}[/]")
        return

    table = Table(title=f"Schema History — {pipeline}/{source_name}")
    table.add_column("Version", style="yellow")
    table.add_column("Fingerprint", style="cyan")
    table.add_column("Columns", style="green")
    table.add_column("Active", style="magenta")
    table.add_column("Discovered At")

    for v in versions:
        table.add_row(
            str(v.version_number),
            v.fingerprint,
            str(len(v.schema_definition.columns)),
            "●" if v.is_active else "",
            str(v.discovered_at),
        )
    console.print(table)

    reg.close()


# ---------------------------------------------------------------------------
# Plugin listing
# ---------------------------------------------------------------------------

plugins_app = typer.Typer(name="plugins", help="Plugin management commands", no_args_is_help=True)
app.add_typer(plugins_app)


@plugins_app.command("list")
def plugins_list() -> None:
    """List all registered source, engine, and sink plugins."""
    from ranger.core.registry import registry

    registry.load_plugins()

    console.print("\n[bold yellow]🤠 Ranger Plugins[/]\n")

    for label, items in [
        ("Sources", registry.list_sources()),
        ("Engines", registry.list_engines()),
        ("Sinks", registry.list_sinks()),
    ]:
        table = Table(title=label)
        table.add_column("Name", style="cyan")
        for name in items:
            table.add_row(name)
        console.print(table)
        console.print()


# ---------------------------------------------------------------------------
# Metadata export
# ---------------------------------------------------------------------------

@app.command("export")
def export_metadata(
    table: str = typer.Option(..., "--table", "-t", help="Metadata table name"),
    output: str = typer.Option(..., "--output", "-o", help="Output file path"),
    format: str = typer.Option("parquet", "--format", "-f", help="Export format: parquet, csv, json"),
    metadata_db: str = typer.Option("ranger_meta.duckdb", "--metadata-db", help="Path to DuckDB metadata file"),
) -> None:
    """Export a metadata table to a file."""
    from ranger.schema.duckdb_registry import DuckDBSchemaRegistry

    reg = DuckDBSchemaRegistry(db_path=metadata_db)
    reg.export_table(table, output, format)
    console.print(f"[green]✓ Exported {table} to {output} ({format})[/]")
    reg.close()


if __name__ == "__main__":
    app()
