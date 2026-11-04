"""Make the South Florida values render with the right confidence.

The tracker does not just store a developer, an architect and a contractor --
it renders each one with a mark saying how strongly it is evidenced, because an
unmarked name reads as verified. Two of those marks were wrong for this market
until this module existed:

1. `developer_canonical` was empty on all 452 rows. The screener falls back to
   `developer`, so the table looked right, but the Overview tab's MOST ACTIVE
   DEVELOPERS chart groups on `developer_canonical` and so read "452 projects in
   this selection have no developer on record". Set here to the LEAD sponsor --
   the first party named -- so a joint venture counts under the firm the press
   names first and Terra's three projects group together instead of appearing as
   three one-off partnership strings.

2. No `field_provenance` row existed for any South Florida project, and
   `app/data.py::mark_team_value` marks a value with no provenance row "·"
   (unverified, carried forward from earlier work and never checked). That is
   the weakest mark in the table, and it was being applied to 309 contractor
   names read directly off the City of Miami's own building permits -- which
   are the strongest kind of evidence this market has.

The tiers written here are the existing vocabulary in `app/data.py::FIELD_TIERS`,
assigned by what the citation actually is:

  registry_confirmed (◉)  a contractor on a government permit record
  document_confirmed (▣)  a value stated in a city's own published record
  web_low_confidence (◇)  a value from one named trade-press article
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text                        # noqa: E402
from db.database import engine, get_session        # noqa: E402
from db.models import Project                      # noqa: E402

CITIES = ("Miami", "Miami Beach", "West Palm Beach", "Coral Gables", "Doral")

# Which tier a citation earns, by what published it. Government records first,
# then the named trade press; anything unrecognised is left for the caller to
# report rather than being quietly tiered.
GOV_MARKERS = ("City of Miami building permit", "Miami-Dade property boundary",
               "City of Doral", "City of Miami Beach", "property appraiser",
               "neighborhoods layer")
PRESS_MARKERS = ("Florida YIMBY", "The Real Deal", "Coral Gables Gazette",
                 "Commercial Observer", "Bisnow", "South Florida Business Journal",
                 "ENR")

# A permit names the contractor as a matter of record, which is a registry
# fact. A city's site-plan record describing a project is a document. Press is
# press. Only the three team fields carry a tier -- those are the fields
# mark_team_value renders.
TEAM_FIELDS = ("architect", "civil_engineer", "general_contractor")


def _tier(filing_name: str, field: str) -> str:
    name = filing_name or ""
    if "building permit" in name.lower():
        return "registry_confirmed" if field == "general_contractor" else "document_confirmed"
    if any(m.lower() in name.lower() for m in GOV_MARKERS):
        return "document_confirmed"
    if any(m.lower() in name.lower() for m in PRESS_MARKERS):
        return "web_low_confidence"
    return ""


def _source_type(filing_name: str) -> str:
    name = (filing_name or "").lower()
    if "permit" in name:
        return "permit_record"
    if any(m.lower() in name for m in PRESS_MARKERS):
        return "web"
    return "board_document"


def lead_sponsor(developer: str) -> str:
    """The first party named in a developer string.

    A joint venture is stored in full in `developer` -- "Terra; RG Development
    Group; GV Development; LORE Development" -- because that is what the source
    said. The canonical name is the lead, which is the party the press names
    first, so the developer chart counts a sponsor's projects together instead
    of once per partnership permutation.
    """
    if not developer:
        return ""
    lead = str(developer).split(";")[0].strip()
    # "Doral Costa Capital LLC (Apollo Cos.; Triarch Capital Group)" -- the
    # parenthetical lists the partners behind the entity, not a second project.
    if "(" in lead:
        lead = lead.split("(")[0].strip()
    return lead


def _developer_method(filing_name: str) -> str:
    """How a developer name was arrived at, in the tracker's own vocabulary.

    Every South Florida developer rests on a single named source -- one trade
    press article or one city record -- so none of them can claim
    `web_corroborated`, which means two independent sources. They fold to the
    `document_only` confidence state, which is the honest one: stated in a
    single document, probably right, not independently corroborated here.
    """
    name = (filing_name or "").lower()
    if any(m.lower() in name for m in PRESS_MARKERS):
        return "web_low_confidence"
    if any(m.lower() in name for m in GOV_MARKERS) or "permit" in name:
        return "document_only"
    return "document_only"


def run(verbose=True):
    db = get_session()
    unknown = set()
    canon = tiers = 0
    try:
        q = ",".join(f"'{c}'" for c in CITIES)
        with engine.connect() as conn:
            cites = {}
            for pid, field, filing in conn.execute(text(
                    "select project_id, field_name, filing_name from extraction_sources "
                    f"where project_id in (select id from projects where city in ({q}))")):
                cites.setdefault((int(pid), field), filing)

        rows = db.query(Project).filter(Project.city.in_(CITIES)).all()
        for row in rows:
            if row.developer:
                row.developer_canonical = lead_sponsor(row.developer)
                row.developer_resolution_method = _developer_method(
                    cites.get((row.id, "developer"), ""))
                canon += 1
        db.commit()

        # field_provenance is written directly: it is a live table with its own
        # supersede/retract columns and no ORM model in this project.
        with engine.begin() as conn:
            conn.execute(text(
                "delete from field_provenance where project_id in "
                f"(select id from projects where city in ({q}))"))
            for row in rows:
                for field in TEAM_FIELDS:
                    value = getattr(row, field)
                    if not value:
                        continue
                    filing = cites.get((row.id, field), "")
                    tier = _tier(filing, field)
                    if not tier:
                        unknown.add(filing or "(no citation)")
                        continue
                    # `outcome` and `source_type` reuse the vocabulary already
                    # in this table: 'resolved' is a value that was found
                    # (mark_team_value only special-cases 'not_yet_selected'),
                    # and the source types are the existing 'web' /
                    # 'board_document', plus 'permit_record' for a building
                    # permit, which no earlier market had as a source.
                    conn.execute(text(
                        "insert into field_provenance "
                        "(project_id, field, value, outcome, tier, source_type, "
                        " source_name, superseded, retracted) "
                        "values (:pid, :f, :v, 'resolved', :t, :st, :n, 0, 0)"),
                        {"pid": row.id, "f": field, "v": str(value),
                         "t": tier, "st": _source_type(filing), "n": filing})
                    tiers += 1
    finally:
        db.close()

    if verbose:
        print(f"developer_canonical set on {canon} row(s); "
              f"{tiers} field_provenance row(s) written")
        if unknown:
            print("  no tier for citation(s): " + "; ".join(sorted(unknown)))
    return {"canonical": canon, "tiers": tiers, "untiered": sorted(unknown)}


if __name__ == "__main__":
    run()
