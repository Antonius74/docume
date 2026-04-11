from collections.abc import Generator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker

from .config import get_settings


settings = get_settings()
engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    from . import models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    _ensure_runtime_schema_updates()


def _ensure_runtime_schema_updates() -> None:
    inspector = inspect(engine)
    if "resources" not in inspector.get_table_names():
        return

    columns = {column["name"] for column in inspector.get_columns("resources")}
    statements: list[str] = []

    if "content_text" not in columns:
        statements.append("ALTER TABLE resources ADD COLUMN content_text TEXT")
    if "canonical_theme" not in columns:
        statements.append("ALTER TABLE resources ADD COLUMN canonical_theme VARCHAR(120)")
    if "author_name" not in columns:
        statements.append("ALTER TABLE resources ADD COLUMN author_name VARCHAR(160)")

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))

        conn.execute(
            text(
                """
                UPDATE resources
                SET canonical_theme = COALESCE(NULLIF(TRIM(canonical_theme), ''), inferred_theme, 'General')
                WHERE canonical_theme IS NULL OR TRIM(canonical_theme) = ''
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE resources
                SET content_text = COALESCE(content_text, summary, description, '')
                WHERE content_text IS NULL
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE resources
                SET author_name = NULLIF(TRIM(author_name), '')
                WHERE author_name IS NOT NULL
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_resources_canonical_theme ON resources (canonical_theme)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_resources_author_name ON resources (author_name)"))
