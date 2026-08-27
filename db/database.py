import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models import Base
# Imported for the side effect of registering the table on Base.metadata,
# so create_all() builds it.
import db.transaction_models  # noqa: F401

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
    _add_column_if_missing("projects", "applicant_source", "VARCHAR")
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
    # The named applicant is not always the party executing the development.
    # A public agency, redevelopment authority or passive landowner is stored
    # here instead of in developer, rather than being discarded.
    _add_column_if_missing("projects", "owner_or_agency", "VARCHAR")
    # Quarantine rather than deletion: excluded rows drop out of every count
    # and chart but stay in the table and stay recoverable.
    _add_column_if_missing("projects", "excluded", "BOOLEAN DEFAULT 0")
    _add_column_if_missing("projects", "excluded_reason", "VARCHAR")
    # Where a square footage came from. A figure a reporter published and a
    # figure the filing stated are not the same kind of fact and must not
    # render alike.
    _add_column_if_missing("projects", "total_gsf_source", "VARCHAR")
    # PERMIT-LEVEL figures, kept apart from the proposed programme on purpose.
    # A building permit states what is being BUILT; an agenda or plan set states
    # what was PROPOSED. They diverge, and merging them into total_gsf would
    # quietly overwrite a reviewed proposal with a construction figure.
    _add_column_if_missing("projects", "permit_gsf", "INTEGER")
    _add_column_if_missing("projects", "permit_living_sf", "INTEGER")
    _add_column_if_missing("projects", "permit_units", "INTEGER")
    _add_column_if_missing("projects", "permit_stories", "INTEGER")
    _add_column_if_missing("projects", "permit_number", "VARCHAR")
    _add_column_if_missing("projects", "permit_issued_date", "VARCHAR")
    _add_column_if_missing("projects", "permit_url", "VARCHAR")
    _add_column_if_missing("projects", "permit_cost", "INTEGER")
    _add_column_if_missing("projects", "general_contractor", "VARCHAR")
    # Delivery, and the evidence for it. A project only reaches Complete or
    # Under Construction from a source OUTSIDE the planning documents -- a
    # permit status, a certificate of occupancy, or corroborated coverage of an
    # opening. Agenda language can never advance a project past Approved,
    # because agendas do not report construction.
    _add_column_if_missing("projects", "completion_stage", "VARCHAR")
    _add_column_if_missing("projects", "completion_basis", "VARCHAR")
    _add_column_if_missing("projects", "completion_evidence", "TEXT")
    _add_column_if_missing("projects", "completion_source_url", "VARCHAR")
    _add_column_if_missing("projects", "completion_date", "VARCHAR")
    # Stale is NOT a stage. It says nothing has been recorded for a long time
    # and no confirming source was found -- which is a different claim from
    # "this was built", and only the first one is supportable.
    _add_column_if_missing("projects", "is_stale", "BOOLEAN DEFAULT 0")
    _add_column_if_missing("projects", "stale_months", "INTEGER")
    # FILING TYPE is not a status. "Lot Merger", "Design Waiver" and
    # "Special Use Permit" describe the ACTION a board was asked to take;
    # status must hold the five normalised pipeline stages and nothing else.
    # Conflating them put eighteen filing actions in a status dropdown.
    _add_column_if_missing("projects", "filing_type", "VARCHAR")
    # Every OTHER address the case is filed under. A Rhode Island application
    # routinely spans several parcels and names them all in the heading; the
    # ingest kept the first, so a project at "311 Knight Street, 321 Knight and
    # 1077 Westminster Street" could not be found by two of its three doors.
    _add_column_if_missing("projects", "alt_addresses", "VARCHAR")
    # How far the stored unit count can be trusted, graded against the
    # documents behind it. A number a reader cannot weigh is worse than a
    # number with a caveat attached.
    _add_column_if_missing("projects", "units_confidence", "VARCHAR")
    # Design and consultant team, kept in separate fields on purpose. DiPrete
    # Engineering appears on a large share of Rhode Island filings and is a
    # CIVIL ENGINEER, not an architect; conflating them would put one firm at
    # the top of an architect ranking it does not belong in.
    _add_column_if_missing("projects", "architect_source", "VARCHAR")
    _add_column_if_missing("projects", "architect_person", "VARCHAR")
    _add_column_if_missing("projects", "surveyor", "VARCHAR")
    _add_column_if_missing("projects", "landscape_architect", "VARCHAR")
    _add_column_if_missing("projects", "attorney", "VARCHAR")
    # development | rezoning. A rezoning petition with no programme yet is
    # the FRONT of the pipeline, an earlier signal than a planning filing,
    # and should not read as a project with a defined programme.
    _add_column_if_missing("projects", "entry_type", "VARCHAR")
    # How review_scale was established, and -- importantly -- whether it is
    # even applicable. A zoning-board variance is not a RIGL 45-23 land
    # development review, so it has no scale to be missing.
    _add_column_if_missing("projects", "review_scale_basis", "VARCHAR")
    # DELIVERED vs TARGET, kept as two columns so a forecast can never be
    # counted as a delivery -- see the note on the model. Real DATE values so
    # the screener sorts chronologically, plus the precision of the period the
    # source actually named, plus the vintage of a forecast.
    _add_column_if_missing("projects", "delivered_date", "DATE")
    _add_column_if_missing("projects", "delivered_precision", "VARCHAR")
    _add_column_if_missing("projects", "target_date", "DATE")
    _add_column_if_missing("projects", "target_precision", "VARCHAR")
    _add_column_if_missing("projects", "target_stated_on", "DATE")
    _add_column_if_missing("projects", "target_stated_by", "VARCHAR")

    # Entity resolution on transactions. `buyer` and `seller` always hold the
    # record entity verbatim; the *_canonical columns hold the resolved sponsor
    # and stay null where it is unresolved, because a blank sponsor is correct
    # and a wrong one poisons the rankings. seller_confidence was missing while
    # buyer_confidence existed -- an asymmetry with no justification.
    _add_column_if_missing("transactions", "seller_confidence", "VARCHAR")
    _add_column_if_missing("transactions", "buyer_resolution_basis", "VARCHAR")
    _add_column_if_missing("transactions", "seller_resolution_basis", "VARCHAR")

    # Repeat sales. The single most useful thing this tracker can show is the
    # same asset trading twice, because a paired basis is a fact about the
    # market that no single transaction is. Stored on the later trade.
    _add_column_if_missing("transactions", "prior_sale_date", "DATE")
    _add_column_if_missing("transactions", "prior_sale_price", "INTEGER")
    _add_column_if_missing("transactions", "prior_sale_source", "VARCHAR")
    _add_column_if_missing("transactions", "basis_change_pct", "FLOAT")
    # A listing broker and a buy-side broker are different facts. Reading
    # "represented the seller" as "brokered both sides" is how a buy-side
    # broker gets erased, and the buy-side broker is often the only clue to
    # what kind of buyer it was.
    _add_column_if_missing("transactions", "broker_buy_side", "VARCHAR")

    # `building_sf` on a spine row is the PARCEL's recorded area. On a
    # condominiumised or fragmented parcel that is a portion of the asset
    # rather than the asset, so the derived $/SF is meaningless -- 160-170 N
    # Washington records 7,200 SF for the 214,000 SF Converse headquarters and
    # computes to $20,833/SF against a reported $800. The price is sound; the
    # denominator is not. This flags the rows where that is demonstrably true
    # rather than deleting a figure that is right on most rows.
    _add_column_if_missing("transactions", "psf_unreliable", "BOOLEAN")

    # A recorded price that the press flatly contradicts. 75 State Street is the
    # case: this table has $325M in September 2019, the press has Rockpoint
    # paying $635M for the building that year, and nothing found reconciles
    # them. The registry figure is kept, because it is what the registry holds,
    # but it must not enter a total or an average as though it were settled.
    _add_column_if_missing("transactions", "price_disputed", "BOOLEAN")
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
