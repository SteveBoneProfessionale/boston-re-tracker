r"""Commercial property transactions: the Acquisitions tab's data model.

Shaped by two facts about Massachusetts that the schema has to carry rather
than hide.

FIRST, a transaction is not always a deed. A sale can move membership interests
in the owning entity, or assign beneficial interests in a nominee trust, and in
both cases the registry shows unchanged title. Those are real transactions and
they are invisible to any deed source. So `transaction_type` is a first-class
column with four values, and a 15% stake can never be summed as though it were
a building:

    asset_sale        whole property, deed recorded
    partial_interest  a percentage of the owning entity changed hands
    entity_level      the owning entity itself was acquired
    distressed        foreclosure deed or deed in lieu -- kept, not dropped,
                      because a lender taking title is a deal in progress

`price` is always what was actually PAID. On a partial interest that is the
amount paid for the stake, never the implied whole-asset value, because the
buyer rankings are computed off `price` and would otherwise be wrong by the
inverse of the percentage. Implied valuation lives in its own column and is
labelled implied wherever it is shown, since it is derived and not a price.

SECOND, price can be checked. Massachusetts deed excise is $2.28 per $500 of
consideration above $100 -- $4.56 per $1,000 -- rounded up to the next $500, so
a stamp implies a price and a stated price implies a stamp. `excise_stamp` and
`excise_implied_price` store both sides so a mismatch is visible. Consideration
excludes assumed debt, so a leveraged deal understates economic value: that is
flagged in `price_caveat`, never silently corrected.

Buyer identity reuses the developer confidence vocabulary rather than inventing
one, so a firm that appears in both tables reads the same way in both.
"""

from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Date, DateTime, Text, ForeignKey,
    UniqueConstraint,
)

from db.models import Base

# What kind of transaction this is. Kept as a vocabulary rather than a boolean
# because "not an asset sale" covers three different things.
TRANSACTION_TYPES = ("asset_sale", "partial_interest", "entity_level", "distressed")

# Why a transaction is not arm's-length. Stored rather than filtered away so a
# reader can see what was excluded and disagree.
NON_ARMS_LENGTH = (
    "nominal_consideration",   # under $100, or "nominal" language
    "love_and_affection",
    "gift",
    "no_excise_stamp",
    "trustees_deed",
    "executors_deed",
    "correction_deed",
    "confirmatory_deed",
    "deed_in_lieu",
    "foreclosure",
    "affiliated_parties",      # grantor and grantee same or related
)


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True)

    # ── identity ────────────────────────────────────────────────────
    address = Column(String)
    parcel_id = Column(String)          # PID in Boston, map_lot/gisid in Cambridge
    city = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)

    # ── the transaction ─────────────────────────────────────────────
    transaction_type = Column(String, default="asset_sale")
    sale_date = Column(Date)
    sale_date_precision = Column(String)   # day | month | quarter | year
    price = Column(Integer)                # what was actually PAID
    price_caveat = Column(String)          # e.g. consideration excludes assumed debt
    buyer = Column(String)
    buyer_canonical = Column(String)       # resolved through the SoC corporations database
    buyer_confidence = Column(String)      # registry_confirmed | web_corroborated |
                                           # web_low_confidence | human_set | null
    seller = Column(String)
    seller_canonical = Column(String)
    seller_confidence = Column(String)     # same vocabulary as buyer_confidence
    # How each side was resolved, so a ranking can be filtered by rigour:
    # pattern | address_cluster | web | registry | human
    buyer_resolution_basis = Column(String)
    seller_resolution_basis = Column(String)
    broker = Column(String)

    # ── partial interest and entity-level ───────────────────────────
    pct_acquired = Column(Float)
    implied_valuation = Column(Integer)    # DERIVED, never a price paid
    is_recapitalization = Column(Boolean, default=False)
    existing_partners = Column(Text)       # remaining partners and their shares

    # ── the property ────────────────────────────────────────────────
    property_type = Column(String)         # from the assessment file, not the deed
    building_sf = Column(Integer)
    unit_count = Column(Integer)
    land_sf = Column(Integer)
    price_per_sf = Column(Float)
    price_per_unit = Column(Float)

    # ── registry record ─────────────────────────────────────────────
    deed_book = Column(String)
    deed_page = Column(String)
    # Torrens registered land is recorded on a Certificate of Title with
    # document and certificate numbers instead of book and page, in a separate
    # database. Roughly a fifth of Suffolk and Middlesex title activity. A
    # schema with only book/page silently cannot represent it.
    is_registered_land = Column(Boolean, default=False)
    certificate_number = Column(String)
    document_number = Column(String)
    doc_type = Column(String)              # Deed, Foreclosure Deed, Deed in Lieu...

    # ── arm's length and excise ─────────────────────────────────────
    arms_length = Column(Boolean)
    non_arms_length_reason = Column(String)
    excise_stamp = Column(Float)
    excise_implied_price = Column(Integer)
    excise_mismatch = Column(Boolean, default=False)

    # ── provenance ──────────────────────────────────────────────────
    source = Column(String)                # cambridge_socrata | suffolk_registry |
                                           # press | sec_filing | broker_release
    source_url = Column(Text)
    source_name = Column(String)
    source_date = Column(String)
    passage = Column(Text)
    confidence = Column(String)            # same vocabulary as buyer_confidence
    notes = Column(Text)

    # ── the link that makes this worth building ─────────────────────
    # A transaction joined to a pipeline project means the project detail can
    # show what the site last traded for, and the transaction can show whether
    # there is an active filing on it.
    linked_project_id = Column(Integer, ForeignKey("projects.id"))
    link_basis = Column(String)            # parcel | address | manual

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("address", "sale_date", "price", "transaction_type",
                         name="uq_transaction"),
    )
