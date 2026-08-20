r"""Record what Cambridge's own registries establish about completion.

cambridge_completion.py joined the tracker's 66 Cambridge rows against three
city datasets. Two kinds of match came back and only one of them is worth
anything.

EXACT, on the building permit number the tracker already stores against the
permit the certificate of occupancy was issued under. That is a key join, not a
guess, and a CO issue date is a completion date from the authority that issued
it. Four projects matched this way.

COARSE, on the planning board special permit. Every one of these was wrong, and
wrong in the same instructive way: PB179 is Cambridge Crossing's master permit
and covers twelve completed buildings from 2008 to 2023, PB303 covers four MIT
Kendall buildings, PB364 two Cambridgeside buildings, PB387 three Alewife Park
buildings. Matching a current row to "its" special permit returns the SIBLINGS
that have finished, never the row itself. Checked by name against the
historical log: not one of our in-progress buildings appears there. Building Q2
is not Building Q1; Alewife Park Building 4 is not Buildings 1 to 3.

A third trap sat in the address join. 150 and 125 Cambridgepark Drive both
match certificates of occupancy at their addresses and both appear in the
historical log by name -- with completion years of 1986 and 1984. Those are the
buildings being redeveloped, not the redevelopments.

    python scraper/cambridge_apply_completion.py --dry-run
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine
from scraper.backfill_delivery_dates import _prov

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

CO_DATASET = "https://data.cambridgema.gov/resource/qwvv-deed.json"

# Exact building-permit joins. type_cert_occ is recorded because it is the
# difference between "the building is finished" and "part of it is occupied",
# and only the first is a delivery in the sense this column means.
CONFIRMED = [
    dict(project_id=414, date="2026-06-01", permit="222345",
         co_type="Entire Building", address="290 Binney St",
         name="MXD Infill - 290 Binney Street (Building C)"),
    dict(project_id=437, date="2026-03-26", permit="216308",
         co_type="Entire Building", address="134 Massachusetts Ave",
         name="Metropolitan Storage Warehouse"),
    dict(project_id=445, date="2026-04-24", permit="239759",
         co_type="Entire Building", address="2 Garden St",
         name="2 Garden Street"),
    dict(project_id=410, date="2025-04-11", permit="250597",
         co_type="Floors/Units", address="57 JFK St",
         name="The Galleria Residential",
         caveat="the certificate covers floors and units rather than the "
                "entire building, so this dates occupancy of part of a "
                "38-unit building rather than a whole-building sign-off"),
]

NULLED = [
    (421, "Rindge Commons Phase 2: 430 Rindge Avenue carries two permit "
          "families. The entire-building certificate of 12 September 2024 was "
          "issued under BLDC-158329-2022, which is Phase 1; a partial "
          "certificate of 30 April 2026 sits under BLDC-261815-2024 and is "
          "probably this phase, but nothing establishes that link -- the row "
          "stores no permit number and the new-construction dataset has no "
          "430 Rindge record. Left null rather than guessed."),
    (430, "150 Cambridgepark Drive matches a certificate of occupancy only by "
          "address, and under a permit (BLDC-93773-2020) that is none of the "
          "three this row stores. The address also appears in the historical "
          "log with a completion year of 1986 -- the building being "
          "redeveloped, not the redevelopment."),
    (436, "125 Cambridgepark Drive matches three certificates by address, none "
          "under this row's permit, and the historical log gives the address a "
          "1984 completion. Same trap as 150 Cambridgepark Drive."),
    (408, "Cambridge Crossing master permit PB179 covers twelve completed "
          "buildings from 2008 to 2023. None of them is this row, which is the "
          "remaining unbuilt master-plan development."),
    (431, "Building Q2. PB179 returns Building Q1, completed 2021, and eleven "
          "other siblings. Q1 is not Q2."),
    (432, "Building R. PB179 returns no Building R; the completed siblings "
          "under that permit are other letters."),
    (412, "MIT Kendall Square Building 2. PB303 returns Buildings 3, 4 and 5 "
          "and the SoMa Garage, completed 2020-2021. Building 2 is not among "
          "them."),
    (413, "MIT Kendall Square Building 6. Same permit, same four completed "
          "siblings, none of them Building 6."),
    (417, "Alexandria PUD 161 First Street. PB243 spans completions from 1982 "
          "to 2024 across the whole PUD."),
    (433, "Cambridgeside 80 First Street. PB364 returns 60 First Street (2023) "
          "and 20 Cambridgeside Place (2025) -- different addresses in the "
          "same redevelopment."),
    (434, "Cambridgeside 150 Cambridgeside Place. PB364 returns 20 "
          "Cambridgeside Place, a different building."),
    (438, "Cambridgeside Core Mall retail. PB364's completions are the two "
          "new buildings, not the mall core."),
    (440, "Alewife Park Building 4. PB387 returns Buildings 1, 2 and 3, all "
          "completed 2025. Building 4 is not among them."),
    (441, "Alewife Park Building 5. Same permit, same three completed "
          "siblings."),
    (442, "Alewife Park parking garage. Same permit, same three completed "
          "buildings, none of them the garage."),
]


def main(dry_run: bool = False):
    conn = engine.connect()
    for c in CONFIRMED:
        log.info("DELIVERED  [%s] %-46s %s  (%s)", c["project_id"],
                 c["name"][:46], c["date"], c["co_type"])
        if dry_run:
            continue
        conn.execute(text(
            "update projects set delivered_date = :d, delivered_precision = 'day',"
            " target_date = null, target_precision = null,"
            " target_stated_on = null, target_stated_by = null where id = :id"),
            {"d": c["date"], "id": c["project_id"]})
        _prov(conn, c["project_id"], "delivered_date", value=c["date"],
              outcome="resolved", tier="registry_confirmed",
              source_type="certificate_of_occupancy", source_url=CO_DATASET,
              source_name=f"City of Cambridge certificates of occupancy, "
                          f"permit {c['permit']}",
              source_date=c["date"],
              passage=f"Certificate of occupancy issued {c['date']} against "
                      f"building permit {c['permit']} at {c['address']}; "
                      f"type: {c['co_type']}.",
              reason=("joined on the building permit number this row already "
                      "stores, so the link to the certificate is exact rather "
                      "than inferred. " + c.get("caveat", "")).strip(),
              step=3)

    for pid, reason in NULLED:
        log.info("null       [%s] %s", pid, reason[:66])
        if dry_run:
            continue
        _prov(conn, pid, "delivered_date", value=None, outcome="null",
              tier=None, source_type="certificate_of_occupancy",
              source_url=CO_DATASET, source_name="City of Cambridge registries",
              source_date="", passage=None, reason=reason, step=3)

    if not dry_run:
        conn.commit()
    conn.close()
    log.info("\n%d confirmed, %d recorded null", len(CONFIRMED), len(NULLED))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    main(**vars(ap.parse_args()))
