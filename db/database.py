import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models import Base

DB_PATH = Path(__file__).parent.parent / "data" / "boston_re.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # needed for Streamlit threading
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def init_db():
    """Create all tables if they don't exist, and apply any pending column additions."""
    Base.metadata.create_all(bind=engine)
    # Add columns introduced after initial schema (SQLite has no ALTER TABLE ADD COLUMN IF NOT EXISTS)
    _add_column_if_missing("projects", "sire_id", "VARCHAR")
    _add_column_if_missing("projects", "developer_canonical", "VARCHAR")
    _add_column_if_missing("projects", "latitude", "FLOAT")
    _add_column_if_missing("projects", "longitude", "FLOAT")
    _add_column_if_missing("projects", "city", "VARCHAR")
    _add_column_if_missing("projects", "equity_partner", "VARCHAR")
    _add_column_if_missing("news_items", "topics", "VARCHAR")
    print(f"Database ready at {DB_PATH}")


def _add_column_if_missing(table: str, column: str, col_type: str):
    with engine.connect() as conn:
        cols = [row[1] for row in conn.execute(
            __import__("sqlalchemy").text(f"PRAGMA table_info({table})")
        )]
        if column not in cols:
            conn.execute(__import__("sqlalchemy").text(
                f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"
            ))
            conn.commit()


def get_session():
    """Return a new database session. Caller is responsible for closing it."""
    return SessionLocal()
