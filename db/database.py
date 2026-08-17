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
    # Cambridge Development Log fields
    _add_column_if_missing("projects", "cambridge_project_id", "VARCHAR")
    _add_column_if_missing("projects", "permit_type", "VARCHAR")
    _add_column_if_missing("projects", "project_type", "VARCHAR")
    _add_column_if_missing("projects", "lot_area", "INTEGER")
    _add_column_if_missing("projects", "far", "FLOAT")
    _add_column_if_missing("projects", "far_scope", "VARCHAR")
    _add_column_if_missing("projects", "affordable_units", "INTEGER")
    _add_column_if_missing("projects", "affordable_units_tbd", "BOOLEAN")
    _add_column_if_missing("projects", "total_gfa_tbd", "BOOLEAN")
    _add_column_if_missing("projects", "hotel_rooms", "INTEGER")
    _add_column_if_missing("projects", "neighborhood_id", "INTEGER")
    _add_column_if_missing("projects", "neighborhood_raw", "VARCHAR")
    _add_column_if_missing("projects", "zoning_raw", "VARCHAR")
    _add_column_if_missing("projects", "zoning_components", "VARCHAR")
    _add_column_if_missing("projects", "notes", "TEXT")
    _add_column_if_missing("projects", "parking_notes", "TEXT")
    _add_column_if_missing("projects", "parent_project_id", "INTEGER")
    _add_column_if_missing("projects", "phase_group", "VARCHAR")
    _add_column_if_missing("projects", "conditional_alternative", "BOOLEAN")
    _add_column_if_missing("projects", "spans_municipalities", "BOOLEAN")
    _add_column_if_missing("projects", "coords_approximate", "BOOLEAN")
    _add_column_if_missing("projects", "special_permit_raw", "VARCHAR")
    _add_column_if_missing("projects", "building_permit_raw", "VARCHAR")
    _add_column_if_missing("projects", "requires_extraction", "BOOLEAN DEFAULT 1")
    # Normalized review scale (replaces the Boston-only project_scale in charts)
    _add_column_if_missing("projects", "review_scale", "VARCHAR")
    _add_column_if_missing("projects", "review_scale_raw", "VARCHAR")
    # Verbatim source asset classification, preserved across canonical folds
    _add_column_if_missing("projects", "asset_class_raw", "VARCHAR")
    # Two-field status: stage heard on an agenda vs stage confirmed in minutes
    _add_column_if_missing("projects", "stage_heard", "VARCHAR")
    _add_column_if_missing("projects", "stage_confirmed", "VARCHAR")
    # Developer provenance: how the name was resolved, and every source URL
    _add_column_if_missing("projects", "developer_resolution_method", "VARCHAR")
    _add_column_if_missing("projects", "developer_sources", "TEXT")
    # Rhode Island parcel identity and extraction fields
    _add_column_if_missing("projects", "assessor_plat", "VARCHAR")
    _add_column_if_missing("projects", "assessor_lots", "VARCHAR")
    _add_column_if_missing("projects", "plat_lots_raw", "VARCHAR")
    _add_column_if_missing("projects", "zoning_district_raw", "VARCHAR")
    _add_column_if_missing("projects", "site_acreage", "FLOAT")
    _add_column_if_missing("projects", "adaptive_reuse", "BOOLEAN")
    _add_column_if_missing("projects", "applicant_entity", "VARCHAR")
    _add_column_if_missing("projects", "case_number", "VARCHAR")
    _add_column_if_missing("projects", "building_count", "INTEGER")
    _add_column_if_missing("projects", "dedupe_review", "BOOLEAN")
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
