"""
windnf - A lightweight package management system for Windows.

This package provides commands and utilities to manage
software repositories and packages.

Modules:
- cli: Command-line interface entry point.
- operations: Core command implementations.
- metadata_manager: Repository metadata handling.
- db_manager: SQLite database interactions.
- config: Configuration management.
- downloader: Download engine abstraction.
"""

from .cli import main

__all__ = ["main"]
