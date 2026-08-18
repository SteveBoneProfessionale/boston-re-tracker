r"""
Rhode Island source registry: which public bodies feed the tracker, and why.

Every board that was considered is recorded here, including the ones that were
deliberately excluded, so a later reader can tell "we decided not to" apart
from "nobody thought of it".

All EntityIDs were verified on 2026-08-17 by fetching the body's own
BoardMembers page and matching the rendered <h1> against the expected name
(scraper/ri_entity_discovery.py), and meeting activity was confirmed against
the dashboard's Past Meetings and Minutes tables
(scraper/ri_board_activity.py).

Portal mechanics worth knowing before using anything here:

  * Meeting tables cap at 100 rows. For a monthly board that is roughly eight
    years of history, which comfortably covers active pipeline, but it is NOT
    complete history and must not be described as such.
  * CURRENT COVERAGE IS SHALLOWER THAN THAT CAP. The harvested corpus holds
    roughly the most recent 18 meetings per board -- about 1.5 years for a
    monthly board -- not the ~100 the portal will serve. This is a deliberate
    first-pass depth, not the ceiling. Any count, total, or chart derived from
    the corpus describes recent activity only, and must not be presented as
    the full available history. See HARVEST_DEPTH below.
  * Meeting dates come from <div id="hdnDateSeq">, not from a text prefix.
  * Document paths come from the DownloadMeetingFiles('...') onclick argument,
    never constructed. The folder number is not the EntityID (Pawtucket Board
    of Appeals is EntityID 2506 and files under folder 4009).
  * The Cancelled Meetings table contains unreliable dates -- a year typo'd as
    2118 on Cranston Planning Commission, and future-dated cancellations on
    Providence Zoning Board of Review. Exclude it from recency logic.
  * EntityIDs are globally alphabetical by board name, so a body can be located
    by binary search; ordering has occasional insertions (Cranston DPRC at 743
    sits out of position), so bracket, then scan locally.

TIERS
  1  Full ingestion. Creates and updates project records.
  2  Zoning relief. Captured and linked to a parcel, but never creates a
     standalone project record -- under the 2023 land use reform, unified
     development review folds much zoning relief into planning board review,
     so these are increasingly supplementary to a Tier 1 filing.
  3  Supporting context. Affects disposition and design, rarely defines
     pipeline scope. Captured and linked, never creates a project record.
"""

# Municipalities in the Rhode Island market.
RI_MUNICIPALITIES = ["Providence", "Cranston", "Pawtucket", "Newport", "Warwick"]

# How deep the harvest currently goes, versus how deep it could go. Recorded so
# that nobody reads a corpus-derived figure as complete history.
HARVEST_DEPTH = {
    "current_meetings_per_board": 18,
    "portal_cap_per_board": 100,
    "current_span_estimate": "~1.5 years for a monthly board",
    "cap_span_estimate": "~8 years for a monthly board",
    "note": (
        "First-pass depth, chosen to get a clean ingestion and see the shape of "
        "the data before committing to a long backfill run. Deepening is a "
        "separate decision -- see the sizing estimate in the project notes."
    ),
}


def _b(entity_id, municipality, name, tier, active, note=""):
    return {
        "entity_id": entity_id,
        "municipality": municipality,
        "name": name,
        "tier": tier,
        "active": active,
        "note": note,
    }


# ── Bodies we ingest ────────────────────────────────────────────────────
BOARDS = [
    # --- Providence ---
    _b(2767, "Providence", "Providence City Plan Commission", 1, True,
       "Primary Providence pipeline source. Assigns case numbers like 26-047MIL; "
       "suffix taxonomy to be derived empirically from agendas, not guessed."),
    _b(2807, "Providence", "Providence Downtown Design Review Committee", 1, True,
       "Active as of 2026-08-03. Absorbing downtown review functions as the "
       "Capital Center Commission has gone dormant -- see EXCLUDED below."),
    _b(1531, "Providence", "I-195 Redevelopment District Commission", 1, True,
       "Statewide-style listing with no municipal name prefix."),
    _b(2934, "Providence", "Providence Zoning Board of Review", 2, True,
       "Cancelled Meetings table carries future-dated entries; ignore it for recency."),
    _b(2873, "Providence", "Providence Redevelopment Agency", 3, True),
    _b(2829, "Providence", "Providence Historic District Commission", 3, True),
    _b(2935, "Providence", "Providence Zoning Commission", 3, True,
       "Distinct from the Zoning Board of Review (2934): this body drafts ordinance "
       "amendments rather than granting relief. Rezoning petitions are parcel-specific "
       "and frequently developer-initiated, which makes them an EARLIER pipeline signal "
       "than a CPC filing. Capture and link to parcel; never create a standalone "
       "project record from a rezoning petition."),

    # --- Cranston ---
    _b(732, "Cranston", "Cranston Planning Commission", 1, True,
       "Cancelled Meetings table contains a year typo'd as 2118 (should be 2018). "
       "Any date-range logic must reject implausible years rather than trust the source."),
    _b(743, "Cranston", "Cranston Development Plan Review Committee (DPRC)", 1, True,
       "EntityID sits out of alphabetical position, presumably renamed after assignment."),
    _b(748, "Cranston", "Cranston Zoning Board of Review", 2, True),
    _b(723, "Cranston", "Cranston Historic District Commission", 3, True),

    # --- Pawtucket ---
    _b(2516, "Pawtucket", "Pawtucket City Planning Commission", 1, True),
    _b(2513, "Pawtucket", "Pawtucket Central Falls District Joint Planning Commission", 1, True,
       "SEPARATE EntityID from 2516, not a mode of it -- both bodies have independent "
       "meeting histories. Because the two sit jointly and their items are documented in "
       "a single combined agenda PDF, 2513 and 2516 are a GUARANTEED duplicate source: "
       "the same document is reachable from both dashboards. Dedupe on document path, "
       "not on board. Last meeting 2025-12-03, so it convenes irregularly."),
    _b(2542, "Pawtucket", "Pawtucket Redevelopment Agency", 3, True),
    _b(2525, "Pawtucket", "Pawtucket Historic District Commission", 3, True),
    _b(2506, "Pawtucket", "Pawtucket Board of Appeals", 2, True,
       "This IS Pawtucket's Zoning Board of Review. There is no separately-listed "
       "'Pawtucket Zoning Board of Review' on the portal -- the full Pawtucket block "
       "(2481-2567) was scanned with no gaps. Confirmed from the 2026-08-03 minutes, "
       "whose header reads: BOARD OF APPEALS / ZONING BOARD OF REVIEW / HOUSING BOARD "
       "OF REVIEW / BUILDING CODE BOARD OF APPEALS / TAX BOARD OF APPEALS. "
       "CAVEAT: because it sits in five capacities, its agendas mix zoning relief with "
       "housing-code, building-code and tax-appeal items. Only zoning relief items are "
       "in scope -- filter on the Pawtucket Zoning Ordinance section citations "
       "(e.g. 410-44 dimensional variance, 410-12.7(B) use variance) rather than "
       "ingesting every item on the agenda."),

    # --- Newport ---
    _b(2204, "Newport", "Newport Planning Board", 1, True,
       "Also the expected surface for Technical Review Committee work -- see EXCLUDED."),
    _b(2240, "Newport", "Newport Zoning Board of Review", 2, True),
    _b(2208, "Newport", "Newport Redevelopment Agency", 3, True),
    _b(2196, "Newport", "Newport Historic District Commission", 3, True),

    # --- Warwick ---
    _b(3765, "Warwick", "Warwick Planning Board", 1, True),
    _b(3802, "Warwick", "Warwick Zoning Board of Review", 2, True),
    _b(3792, "Warwick", "Warwick Station Redevelopment Agency", 3, True,
       "Scope is the Warwick Station TOD district specifically, not citywide."),
]

# ── Bodies deliberately NOT ingested ────────────────────────────────────
# Recorded so the decision stays legible, and so nobody re-adds them later
# thinking they were simply overlooked.
EXCLUDED = [
    {
        "entity_id": 382,
        "name": "Capital Center Commission",
        "municipality": "Providence",
        "reason": (
            "DORMANT. Last meeting 2025-07-09 -- 404 days before verification on "
            "2026-08-17 -- against a prior record of regular meetings back to 2018. "
            "Providence's 2024 Comprehensive Plan contemplated dissolving this body and "
            "consolidating Downtown review, and the meeting record is consistent with "
            "that having happened in practice. The Downtown Design Review Committee "
            "(2807) is active and appears to have taken up the function. "
            "Listed statewide-style with no 'Providence' prefix, which is why it does "
            "not appear in the Providence block (2749-2939). "
            "Revisit if it resumes filing."
        ),
    },
    {
        "entity_id": 2768,
        "name": "Providence City Plan Commission Sub-Committee for Referral Review",
        "municipality": "Providence",
        "reason": (
            "Duplicate coverage. Referred items return to the full CPC (2767), so this "
            "body surfaces no projects that 2767 will not, while adding dedup load."
        ),
    },
    {
        "entity_id": None,
        "name": "Newport Technical Review Committee",
        "municipality": "Newport",
        "reason": (
            "NOT PRESENT ON THE PORTAL. Confirmed two ways: absent from the complete "
            "Newport block (2180-2243, which ends at 'Newport, City of'), and absent as "
            "an unprefixed listing -- the global alphabetical sequence runs 3464 "
            "'Technical Assistance Committee, Governor's' straight to 3465 'Teen "
            "Pregnancy Prevention Partnership'. Newport's TRC is a staff-level body that "
            "reviews ahead of the Planning Board and does not appear to file separately "
            "under the Open Meetings Act. Its work should surface inside Planning Board "
            "(2204) agendas."
        ),
    },
    {
        "entity_id": None,
        "name": "Pawtucket Zoning Board of Review",
        "municipality": "Pawtucket",
        "reason": (
            "Does not exist under that name. The function is performed by the Pawtucket "
            "Board of Appeals (2506), which is ingested as Tier 2."
        ),
    },
]


def boards_for(municipality: str, tier: int | None = None) -> list[dict]:
    return [
        b for b in BOARDS
        if b["municipality"] == municipality and (tier is None or b["tier"] == tier)
    ]


def by_tier(tier: int) -> list[dict]:
    return [b for b in BOARDS if b["tier"] == tier]


if __name__ == "__main__":
    for muni in RI_MUNICIPALITIES:
        rows = boards_for(muni)
        print(f"\n=== {muni.upper()} ({len(rows)}) ===")
        for b in sorted(rows, key=lambda x: (x["tier"], x["entity_id"])):
            print(f"  T{b['tier']}  {b['entity_id']:<6} {b['name']}")
    print(f"\n=== EXCLUDED ({len(EXCLUDED)}) ===")
    for e in EXCLUDED:
        print(f"  {str(e['entity_id'] or '—'):<6} {e['name']}")
    print(f"\nTier 1: {len(by_tier(1))}   Tier 2: {len(by_tier(2))}   Tier 3: {len(by_tier(3))}")

# EXTRACTION METHOD (recorded so the code's shape is legible later)
# ---------------------------------------------------------------
# Field extraction is DETERMINISTIC and PER-CITY (scraper/ri_segment.py +
# scraper/ri_extract_items.py), not model-read. The Boston pipeline reads
# filings with an LLM; Rhode Island does not, because the Console API credit
# was unavailable when this was built. The documents were read by hand and
# each municipality's real item structure encoded as a segmenter:
#
#   Providence  numbered items under section headers, plus a separately
#               parsed administrative-approvals run-on
#   I-195       inline numbered items citing District parcels, not plats
#   Pawtucket   stage line with a vote marker, then address, then narrative
#   Newport     application number, then parcel/zoning line
#   Warwick     lettered items with a labelled key/value block
#   Cranston    bulleted items with a labelled key/value block
#
# If API credit becomes available, the segmenters are the part to replace:
# hand the model the whole agenda and let it return item boundaries. The
# extraction, identity, dedup and citation layers downstream are independent
# of how segmentation happens and would not need to change.
