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

    # Extracted fields (populated after PDF processing)
    developer = Column(String)
    developer_canonical = Column(String)          # normalized parent company name
    asset_class = Column(String)
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

    # Relationships
    filings = relationship("ProjectFiling", back_populates="project",
                           cascade="all, delete-orphan")
    extraction_sources = relationship("ExtractionSource", back_populates="project",
                                      cascade="all, delete-orphan")
    news_items = relationship("NewsItem", back_populates="project")
    flags = relationship("FlaggedExtraction", back_populates="project",
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
