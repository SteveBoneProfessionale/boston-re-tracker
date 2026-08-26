r"""Give the Boston spine a real asset class instead of a bare use code.

"Asset class populated on 100% of rows" was true and misleading. On the 653
Boston spine rows the value was the string "Commercial (320)" -- the DOR
property-type code wrapped in the word Commercial, which is the banding the
loader used to decide the row was commercial in the first place. It says
nothing. You cannot filter for offices, you cannot compute a median $/SF for
retail, and the Asset Class column on the tab was 73 rows of "Commercial (320)"
sitting next to Cambridge's "GEN-OFFICE" and press's "Office".

Boston publishes the mapping itself: every annual assessment file carries LUC
alongside LU_DESC, so 320 is RET/WHSL/SERVICE, 340 is GENERAL OFFICE, 300 is
HOTEL. No code table has to be guessed, and guessing was the alternative --
DOR's ranges are not evenly spaced and 316, 332 and 357 are not inferable.

Both parts are kept: "Retail/Whsl/Service (320)" rather than either alone, so
the tab reads in English and the row can still be joined back to the code.

    python scraper/acq_asset_class.py --apply
"""

import argparse
import csv
import io
import logging
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

UA = {"User-Agent": "boston-re-tracker/1.0 (13silonergan@gmail.com)"}
# FY2023 is used because Boston's MassGIS L3 layer is FY2023, so the use codes
# on the transactions and the descriptions here come from the same vintage.
LU_DUMP = ("https://data.boston.gov/datastore/dump/"
           "1000d81c-5bb5-49e8-a9ab-44cd042f1db2?format=csv&fields=LUC,LU_DESC")


def _title(s: str) -> str:
    """HOTEL -> Hotel, RET/WHSL/SERVICE -> Ret/Whsl/Service."""
    out = []
    for part in s.replace("_", " ").split():
        out.append("/".join(w.capitalize() if w.isalpha() else w
                            for w in part.split("/")))
    return " ".join(out)


def fetch_lu() -> dict:
    with httpx.Client(headers=UA, timeout=180, follow_redirects=True) as c:
        r = c.get(LU_DUMP)
        r.raise_for_status()
        text_ = r.text
    lu = {}
    for row in csv.DictReader(io.StringIO(text_)):
        code = (row.get("LUC") or "").strip()
        desc = (row.get("LU_DESC") or "").strip()
        if code and desc and code not in lu:
            lu[code] = desc
    return lu


def main(dry_run: bool):
    lu = fetch_lu()
    log.info("%d land-use codes with a description", len(lu))

    conn = engine.connect()
    rows = conn.execute(text(
        "select id, property_type from transactions "
        "where source = 'massgis_l3' and (property_type like 'Commercial (%' or property_type like 'Industrial (%')"
    )).fetchall()
    log.info("%d Boston rows carrying a bare use code", len(rows))

    changed, unknown = 0, {}
    for rid, pt in rows:
        code = pt.split("(")[-1].rstrip(")").strip()
        desc = lu.get(code)
        if not desc:
            unknown[code] = unknown.get(code, 0) + 1
            continue
        label = f"{_title(desc)} ({code})"
        if not dry_run:
            conn.execute(text(
                "update transactions set property_type = :p where id = :id"),
                {"p": label, "id": rid})
        changed += 1
    if not dry_run:
        conn.commit()

    log.info("%d relabelled", changed)
    if unknown:
        log.info("codes with no description in the FY2023 file: %s",
                 dict(sorted(unknown.items(), key=lambda x: -x[1])))

    log.info("\ntop asset classes, Boston spine:")
    for v, n in conn.execute(text(
            "select property_type, count(*) from transactions "
            "where source = 'massgis_l3' group by 1 order by 2 desc limit 12")):
        log.info("  %4d  %s", n, v)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
