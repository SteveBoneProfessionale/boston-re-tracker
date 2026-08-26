r"""Make a firm one entity across development and acquisitions.

A sponsor that develops in the Projects tab and buys in the Acquisitions tab
should read as one firm, not two. This walks the 54 resolved transaction
sponsors against the 873 developer names already in the tracker and writes
`developer_canonical` on the project rows that match.

SUBSTRING MATCHING IS NOT MATCHING. The first attempt normalised both sides and
tested `in`, which produced:

    MIT    == Smithfield Properties      (mit inside sMITHfield)
    Hines  == Chinese Consolidated ...   (hines inside cHINESe)

Both are the same failure as the HARVARD-street problem earlier: short stems
appear inside longer unrelated words. So matching here is TOKEN-ALIGNED. The
sponsor's tokens must appear as a contiguous run of whole tokens in the
developer string, and a single-token sponsor shorter than four characters is
never matched on its own.

Developer strings in this tracker are frequently compound -- "Seaport Square
Development Company LLC, an affiliate of W/S Development Associates LLC" names
two entities. A contiguous token run still finds the real firm inside them
without the substring collisions.

    python scraper/acq_developer_crosswalk.py --apply
"""

import argparse
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# ONLY legal-form suffixes are stripped. An earlier version also stripped
# descriptive words -- Management, Associates, Properties, Real, Estate -- and
# that is precisely what distinguishes one firm from another:
#
#     Alpha Management   vs  Alpha Associates      different firms
#     Eastern Real Estate vs Eastern Plumbing Co.  different firms
#
# With those words removed both pairs collapse to a single shared token and
# match. They are kept.
NOISE = {"the", "co", "corp", "corporation", "inc", "llc", "l", "lp", "llp",
         "ltd", "company", "of", "and", "an", "affiliate", "its"}


def tokens(s: str) -> list:
    t = re.sub(r"[^a-z0-9 ]", " ", (s or "").lower())
    return [w for w in t.split() if w]


def core(s: str) -> list:
    return [w for w in tokens(s) if w not in NOISE]


def _run(needle: list, hay: list) -> bool:
    n = len(needle)
    return n > 0 and any(hay[i:i + n] == needle for i in range(len(hay) - n + 1))


def matches(sponsor: str, developer: str) -> bool:
    """True when either name's tokens form a contiguous whole-token run in the
    other.

    Bidirectional because the tracker's two sides abbreviate differently:
    the transaction sponsor is "Alexandria Real Estate Equities" while the
    project developer is "Alexandria Real Estate" -- neither contains the other
    in one direction only.
    """
    sc, dc = core(sponsor), core(developer)
    if not sc or not dc:
        return False
    # A lone short token (MIT, WS, BXP) is too collision-prone to match on a
    # run; require the whole name to be that token.
    if len(sc) == 1 and len(sc[0]) < 4:
        return dc == sc
    return _run(sc, dc) or _run(dc, sc)


def main(dry_run: bool):
    conn = engine.connect()
    sponsors = sorted({r[0].strip() for col in ("buyer_canonical", "seller_canonical")
                       for r in conn.execute(text(
                           f"select distinct {col} from transactions "
                           f"where coalesce({col},'') <> ''"))})
    devs = conn.execute(text(
        "select id, developer, coalesce(developer_canonical,'') from projects "
        "where coalesce(developer,'') <> ''")).fetchall()
    log.info("%d resolved sponsors vs %d project developer rows",
             len(sponsors), len(devs))

    pairs, n = {}, 0
    for pid, dev, canon in devs:
        for sp in sponsors:
            if matches(sp, dev):
                pairs.setdefault(sp, set()).add(dev.strip())
                if not dry_run and not canon:
                    conn.execute(text(
                        "update projects set developer_canonical = :s "
                        "where id = :id"), {"s": sp, "id": pid})
                n += 1
                break
    if not dry_run:
        conn.commit()

    log.info("\n%d sponsors matched a developer name, %d project rows tagged",
             len(pairs), n)
    for sp in sorted(pairs):
        log.info("  %-40s", sp)
        for d in sorted(pairs[sp])[:3]:
            log.info("        %s", d[:74])
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
