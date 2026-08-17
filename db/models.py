from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime,
    ForeignKey, Text, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True)
    bpda_url = Column(String, unique=True, nullable=False)  # unique project page URL
    name = Column(String)
    address = Column(String)
    neighborhood = Column(String)
    status = Column(String)                    # raw status from BPDA index
    bpda_gsf = Column(Integer)                 # gross floor area from BPDA detail page (pre-extraction)
    project_scale = Column(String)             # "Large Project" | "Small Project" | None (from BPDA tags)
    skip_reason = Column(String)               # why this project was excluded from PDF processing

    # Normalized review scale. `review_scale` is the value the Review Scale chart
    # reads -- one column for every market, so the chart never branches on city.
    # Each market's permitted values are declared in app/data.py::MARKETS
    # ("review_scale_vocab"); a market whose statute has no scale classification
    # (Cambridge) declares None, which the UI renders as "not applicable" rather
    # than as an empty chart. `review_scale_raw` keeps the source's verbatim
    # wording for the detail view, same split as stage vs. status.
    review_scale = Column(String)
    review_scale_raw = Column(String)

    # Extracted fields (populated after PDF processing)
    developer = Column(String)
    developer_canonical = Column(String)          # normalized parent company name
    asset_class = Column(String)                  # canonical value only -- see app/data.py::ASSET_CLASSES
    asset_class_raw = Column(String)              # verbatim source classification before canonicalization,
                                                    # so a fold (e.g. Cambridge's "Fire Department" ->
                                                    # "Institutional") stays recoverable. Same split as
                                                    # review_scale vs review_scale_raw.
    total_gsf = Column(Integer)
    residential_units = Column(Integer)
    commercial_gsf = Column(Integer)
    building_height_ft = Column(Float)
    num_stories = Column(Integer)
    parking_spaces = Column(Integer)
    architect = Column(String)
    civil_engineer = Column(String)
    expected_delivery = Column(String)
    project_status_filing = Column(String)
    description = Column(Text)

    # Which filing was processed
    processed_filing_url = Column(String)
    processed_filing_name = Column(String)
    processed_filing_type = Column(String)     # dpir / pnf / small_project

    # Extraction metadata
    extraction_model = Column(String)
    extraction_timestamp = Column(DateTime)
    extraction_superseded = Column(Boolean, default=False)
    requires_extraction = Column(Boolean, default=True)  # False for markets ingested from
                                                           # structured data (e.g. Cambridge) --
                                                           # lets chart code check "has financial
                                                           # data ready" without checking city name

    # SIRE / Salesforce
    sire_id = Column(String)                    # Salesforce Plan__c / Project__c ID

    # Geo
    latitude = Column(Float)
    longitude = Column(Float)
    city = Column(String)                       # "Boston" for BPDA projects, actual city for manual

    # Deal metadata
    equity_partner = Column(String)             # equity partner / capital partner if known

    # Scrape metadata
    first_seen_date = Column(DateTime, default=datetime.utcnow)
    last_checked_date = Column(DateTime)
    is_flagged = Column(Boolean, default=False)

    # --- Cambridge Development Log fields (city == "Cambridge") ---
    # bpda_url stays NOT NULL/UNIQUE as originally defined; Cambridge rows get a
    # synthetic "cambridge://{project_id}" value there so that constraint holds
    # without an in-place SQLite column-constraint rewrite. cambridge_project_id
    # below is the real upsert key for Cambridge ingestion.
    cambridge_project_id = Column(String, unique=True)    # Socrata project_id, e.g. "821"
    permit_type = Column(String)                # Planning Board Special Permit / BZA / AHO / etc.
    project_type = Column(String)                # raw compound value, e.g. "Alteration/Change of Use"
    lot_area = Column(Integer)
    far = Column(Float)
    far_scope = Column(String)                   # "building" | "pud" -- FAR is often reported for a whole PUD
    affordable_units = Column(Integer)
    affordable_units_tbd = Column(Boolean, default=False)
    total_gfa_tbd = Column(Boolean, default=False)
    hotel_rooms = Column(Integer)
    neighborhood_id = Column(Integer)             # 1-13, normalized off the leading number
    neighborhood_raw = Column(String)             # source string before normalization
    zoning_raw = Column(String)
    zoning_components = Column(String)            # comma-joined parsed zoning tokens
    notes = Column(Text)
    parking_notes = Column(Text)
    parent_project_id = Column(Integer, ForeignKey("projects.id"), nullable=True)
    phase_group = Column(String)                  # text before " - " in "Parent - Component" names, e.g.
                                                    # "Kendall Common", "Alewife Park" -- for UI grouping even
                                                    # when no literal parent row exists to FK against
    conditional_alternative = Column(Boolean, default=False)   # e.g. PB315 MA2 vs MA3 competing plans
    spans_municipalities = Column(Boolean, default=False)      # e.g. Cambridge Crossing also in Somerville/Boston
    coords_approximate = Column(Boolean, default=False)
    special_permit_raw = Column(String)           # e.g. "PB315 MA2"
    building_permit_raw = Column(String)          # e.g. "195497, 195498 (Bldg A)"

    # Relationships
    filings = relationship("ProjectFiling", back_populates="project",
                           cascade="all, delete-orphan")
    extraction_sources = relationship("ExtractionSource", back_populates="project",
                                      cascade="all, delete-orphan")
    news_items = relationship("NewsItem", back_populates="project")
    flags = relationship("FlaggedExtraction", back_populates="project",
                         cascade="all, delete-orphan")
    parent = relationship("Project", remote_side=[id], foreign_keys=[parent_project_id])
    cambridge_uses = relationship("CambridgeProjectUse", back_populates="project",
                                  cascade="all, delete-orphan")
    cambridge_building_permits = relationship("CambridgeBuildingPermit", back_populates="project",
                                              cascade="all, delete-orphan")
    cambridge_special_permits = relationship("CambridgeSpecialPermit", back_populates="project",
                                             cascade="all, delete-orphan")
    cambridge_aliases = relationship("CambridgeProjectAlias", back_populates="project",
                                     cascade="all, delete-orphan")


class ProjectFiling(Base):
    __tablename__ = "project_filings"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    name = Column(String)
    date = Column(String)
    url = Column(String)
    file_type = Column(String)                 # pdf, doc, etc.
    filing_category = Column(String)           # dpir, pnf, small_project, other
    is_processed = Column(Boolean, default=False)

    __table_args__ = (UniqueConstraint("project_id", "url", name="uq_filing_url"),)

    project = relationship("Project", back_populates="filings")


class ExtractionSource(Base):
    __tablename__ = "extraction_sources"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    field_name = Column(String)
    field_value = Column(Text)
    filing_name = Column(String)
    filing_date = Column(String)
    pdf_url = Column(String)
    page_number = Column(Integer)

    project = relationship("Project", back_populates="extraction_sources")


class NewsItem(Base):
    __tablename__ = "news_items"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    url = Column(String, unique=True)
    published_date = Column(DateTime)
    source = Column(String)                    # bpda | boston_gov
    summary = Column(Text)
    linked_project_id = Column(Integer, ForeignKey("projects.id"), nullable=True)
    match_score = Column(Float)
    topics = Column(String)                     # comma-separated topic tags, e.g. "Construction,Financing"

    project = relationship("Project", back_populates="news_items")


class DeveloperCache(Base):
    """Permanent cache: raw LLC name → canonical developer name."""
    __tablename__ = "developer_cache"

    id = Column(Integer, primary_key=True)
    raw_name = Column(String, unique=True, nullable=False)
    canonical_name = Column(String, nullable=False)
    resolved_by = Column(String)                  # "rules" | "ai"


class FlaggedExtraction(Base):
    __tablename__ = "flagged_extractions"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    field_name = Column(String)
    user_note = Column(Text)
    flagged_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default="open")    # open | resolved | re-extract
    current_value = Column(Text)
    source_pdf_url = Column(String)

    project = relationship("Project", back_populates="flags")


class CambridgeProjectUse(Base):
    """One row per use category within a Cambridge project (project can have multiple)."""
    __tablename__ = "cambridge_project_uses"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    use_category = Column(String)                # normalized: Office/R&D, Lab/R&D, Residential, Retail, Hotel,
                                                   # Institutional, Educational, Mixed Use, Community Center,
                                                   # Fire Department, Parking Garage, Other
    use_category_raw = Column(String)
    gfa = Column(Integer)
    gfa_tbd = Column(Boolean, default=False)
    is_fallback = Column(Boolean, default=False)  # True when derived from primary_use+total_gfa rather than
                                                   # a real per-use breakdown from the (currently stale) use table

    project = relationship("Project", back_populates="cambridge_uses")


class CambridgeBuildingPermit(Base):
    __tablename__ = "cambridge_building_permits"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    permit_number = Column(String)
    label = Column(String)                        # e.g. "Bldg A", or None

    project = relationship("Project", back_populates="cambridge_building_permits")


class CambridgeSpecialPermit(Base):
    __tablename__ = "cambridge_special_permits"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    base_permit = Column(String)                  # e.g. "PB315"
    amendment_raw = Column(String)                # e.g. "MA2", "Amend 7", "Amd 7", or None
    full_raw = Column(String)                      # original full string, e.g. "PB315 MA2"

    project = relationship("Project", back_populates="cambridge_special_permits")


class CambridgeProjectAlias(Base):
    __tablename__ = "cambridge_project_aliases"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    former_name = Column(String)
    date_observed = Column(DateTime, default=datetime.utcnow)

    project = relationship("Project", back_populates="cambridge_aliases")


class CambridgeQuarterlySnapshot(Base):
    """Full raw row captured before normalization, so nothing is lost to a parsing bug."""
    __tablename__ = "cambridge_quarterly_snapshots"

    id = Column(Integer, primary_key=True)
    cambridge_project_id = Column(String, nullable=False)   # Socrata project_id, independent of internal FK
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True)
    edition = Column(String)                       # e.g. "2026 Q1"
    full_row_json = Column(Text)
    ingested_at = Column(DateTime, default=datetime.utcnow)
