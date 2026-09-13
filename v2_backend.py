"""V2 Backend Selection and Connection Management.

Determines which V2StateRepository implementation to use at startup:
    DATABASE_URL present → PostgresV2StateRepository (with migration runner)
    DATABASE_URL absent  → SQLiteV2StateRepository
    Postgres configured but unavailable → fail closed (no silent SQLite fallback)

Usage in craft_v2.py:
    from v2_backend import init_v2_backend
    v2_repo = init_v2_backend(db)
"""

from __future__ import annotations

import logging
import os
from typing import Optional

log = logging.getLogger("craft.v2_backend")


def init_v2_backend(db=None, *, database_url: Optional[str] = None):
    """Initialize the V2 state backend.

    1. Determine backend from DATABASE_URL env var (or explicit parameter).
    2. If Postgres: run pending migrations, then create repository.
    3. If SQLite: create repository with the shared db connection.
    4. Verify health (fail closed if Postgres is unreachable).

    Args:
        db: SQLite Database instance (required if not using Postgres).
        database_url: Override for DATABASE_URL env var. Primarily for testing.

    Returns:
        V2StateRepository instance.

    Raises:
        ConnectionError: If Postgres is configured but unreachable.
        ImportError: If psycopg is missing and Postgres is configured.
    """
    from v2_state_repository import (
        PostgresV2StateRepository,
        SQLiteV2StateRepository,
        create_v2_repository,
        run_postgres_migrations,
    )

    url = database_url or os.environ.get("DATABASE_URL")

    if url:
        log.info("V2 backend: Postgres selected (DATABASE_URL present)")

        # Run pending migrations before creating the repository
        migrations_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "migrations",
            "v2_postgres",
        )
        try:
            applied = run_postgres_migrations(url, migrations_dir)
            if applied > 0:
                log.info(f"Applied {applied} Postgres migration(s)")
            else:
                log.info("Postgres schema up to date")
        except Exception as e:
            raise ConnectionError(
                f"Failed to run Postgres migrations: {e}. "
                "V2 backend cannot start — fail closed."
            ) from e

        # Create repository (this also verifies connectivity)
        repo = create_v2_repository(database_url=url)
        health = repo.health_check()
        log.info(
            f"Postgres V2 backend ready: {health.get('tables_found', [])} "
            f"(schema v{health.get('schema_version', '?')})"
        )
        return repo

    else:
        log.info("V2 backend: SQLite selected (no DATABASE_URL)")
        if db is None:
            raise ValueError(
                "SQLite V2 backend requires a Database instance. "
                "Pass the shared db object to init_v2_backend()."
            )
        repo = create_v2_repository(db=db)
        health = repo.health_check()
        log.info(f"SQLite V2 backend ready: {health.get('tables_found', [])}")
        return repo
