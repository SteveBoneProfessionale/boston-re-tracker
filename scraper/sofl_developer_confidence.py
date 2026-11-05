"""Re-run the developer confidence tier for South Florida.

WHY IT WAS WRONG. Every South Florida developer name was written by the press
research passes and stamped `web_low_confidence`, which the panel rendered as
"DOCUMENT ONLY · WEB_LOW_CONFIDENCE". For the Waldorf Astoria that put a
low-confidence badge on Property Markets Group -- a name Commercial Observer
and The Real Deal both carry in the 2024 loan coverage, and which the County's
own owner of record corroborates as PMG DOWNTOWN DEVELOPERS LP. A name
attested by press AND by the deed is not low confidence.

THE TEST. A row is upgraded to `web_corroborated`, which the panel reads as
"Confirmed by permit and press", when BOTH hold:

  1. a press citation exists for the developer field -- an ExtractionSource
     row, which the findings validator only writes against a quoted sentence
     from an acceptable outlet; and
  2. the County's OWNER OF RECORD agrees with that name.

Agreement is tested two ways, because a developer almost never owns a parcel
under its trading name:

  TOKEN   a distinctive word of four or more letters appears in both, so
          "Related Group" matches "RELATED URBAN CONSTRUCTION LLC".
  INITIAL the developer's initials appear as a whole token in the owner
          entity, so "Property Markets Group" matches "PMG DOWNTOWN
          DEVELOPERS LP". This is the common case and the one that made the
          Waldorf look unsourced.

A row failing either test keeps the tier it had. The point is to stop
understating what is actually corroborated, not to promote everything.

    python -m scraper.sofl_developer_confidence --dry-run
    python -m scraper.sofl_developer_confidence
"""

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from db.database import get_session, init_db                  # noqa: E402
from db.models import Project, ExtractionSource               # noqa: E402
from scraper import sofl_changelog as clog                    # noqa: E402

CITIES = ("Miami", "Miami Beach", "West Palm Beach", "Coral Gables", "Doral")

# Words carrying no identifying force in a company name.
_NOISE = {"the", "group", "groups", "llc", "inc", "corp", "corporation", "co",
          "company", "companies", "cos", "ltd", "lp", "llp", "partners",
          "partnership", "development", "developments", "developers",
          "developer", "holdings", "holding", "properties", "property",
          "realty", "real", "estate", "capital", "investments", "investment",
          "ventures", "venture", "management", "associates", "and", "of",
          "miami", "florida", "fl", "usa", "us", "international", "trust",
          "owner", "land", "propco", "opco", "lllp", "limited"}

_WORD = re.compile(r"[A-Za-z][A-Za-z&']*")


def _tokens(name: str) -> list[str]:
    return [w.lower() for w in _WORD.findall(str(name or ""))]


def _distinctive(name: str) -> set[str]:
    return {t for t in _tokens(name) if len(t) >= 4 and t not in _NOISE}


def _initials(name: str) -> str:
    """'Property Markets Group' -> 'PMG'. Noise words still count here.

    Initials are taken over the FULL name rather than the distinctive words,
    because that is how firms build their abbreviations -- Property Markets
    Group is PMG, not PM.
    """
    words = [t for t in _tokens(name) if t not in ("the", "and", "of")]
    return "".join(w[0] for w in words).upper()


def agrees(developer: str, owner: str) -> str:
    """'' if the two do not corroborate, else which test matched."""
    if not developer or not owner:
        return ""
    # A developer field may list several firms; any one of them agreeing is
    # enough, because the owner entity belongs to the lead sponsor.
    for part in re.split(r"[;,]| and ", developer):
        part = part.strip()
        if not part:
            continue
        if _distinctive(part) & _distinctive(owner):
            return "token"
        ini = _initials(part)
        if len(ini) >= 2 and ini in [t.upper() for t in _tokens(owner)]:
            return "initials"
    return ""


def run(dry_run=False, verbose=True):
    init_db()
    db = get_session()
    entries = clog.load()
    upgraded, left = [], []
    try:
        rows = db.query(Project).filter(
            Project.city.in_(CITIES),
            Project.developer.isnot(None)).order_by(Project.id).all()
        cited = {r[0] for r in db.query(ExtractionSource.project_id).filter(
            ExtractionSource.field_name == "developer").distinct()}
        for row in rows:
            method = (row.developer_resolution_method or "").strip()
            if method not in ("web_low_confidence", "", "registry_self"):
                continue
            name = row.developer_canonical or row.developer
            why = agrees(name, row.owner_or_agency or "")
            if not why or row.id not in cited:
                reason = ("no press citation for the developer field" if why
                          else "no owner of record on the row"
                          if not (row.owner_or_agency or "").strip()
                          else "owner of record does not corroborate the name")
                left.append((row.id, row.name, name, row.owner_or_agency, reason))
                continue
            upgraded.append((row.id, row.name, name, row.owner_or_agency, why))
            clog.record(entries, row.id, row.name, "developer_resolution_method",
                        method or None, "web_corroborated", "",
                        f"the developer is cited to an acceptable outlet AND the County's "
                        f"owner of record corroborates it by {why}, so the name is "
                        f"confirmed rather than low-confidence")
            if not dry_run:
                row.developer_resolution_method = "web_corroborated"
                row.developer_confidence = "confirmed"
        if not dry_run:
            db.commit()
    finally:
        db.close()
    if not dry_run:
        clog.save(entries)
    if verbose:
        print(f"{'(dry run) ' if dry_run else ''}{len(upgraded)} upgraded to "
              f"web_corroborated, {len(left)} left as they were\n")
        for pid, proj, dev, owner, why in upgraded[:14]:
            print(f"  {pid:>5} {str(proj)[:30]:<31}{str(dev)[:26]:<27}"
                  f"{str(owner)[:28]:<29}{why}")
        if len(upgraded) > 14:
            print(f"  ... and {len(upgraded) - 14} more")
        by_reason = {}
        for *_, reason in left:
            by_reason[reason] = by_reason.get(reason, 0) + 1
        if by_reason:
            print("\n  not upgraded:")
            for reason, n in sorted(by_reason.items(), key=lambda kv: -kv[1]):
                print(f"    {n:>4}  {reason}")
    return upgraded, left


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(**vars(ap.parse_args()))
