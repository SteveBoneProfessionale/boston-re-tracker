"""Apply researched field values to South Florida rows, with the checks that
make a reported finding trustworthy enough to store.

Research that arrives as a report rather than as a document has one failure
mode worth designing against: a value that reads plausibly and is not in the
source. So nothing here is taken on trust. Every value must arrive with

  * a source URL, and
  * the verbatim sentence from that source that states it,

and the sentence is then checked against the value. A contractor "KAST
Construction" whose quote never says KAST is rejected, not stored, because the
cheapest moment to catch an invented value is before it enters the table.

Three further guards, each for a rule the brief is strict about:

  LENDER      rejected when the quote describes a refinancing, an acquisition
              loan, a predevelopment loan or preferred equity -- none of those
              is the construction lender, and loan coverage is mostly not
              construction financing.
  ARCHITECT   rejected when the quote names the firm as a landscape architect
              or an interior designer.
  HEIGHT      flagged when the quote mentions sea level, because an FAA letter
              gives both numbers and only the above-ground one is wanted.

This module ADDS to a row and never clears a field, so it is safe over the
permit-derived Miami rows whose contractor and area come from the City.

    python -m scraper.sofl_apply_findings data/sofl/agent_findings.json
    python -m scraper.sofl_apply_findings data/sofl/agent_findings.json --dry-run
"""

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import get_session, init_db       # noqa: E402
from db.models import Project, ExtractionSource    # noqa: E402
from scraper import sofl_changelog as clog         # noqa: E402

COLUMNS = {
    "name": "name",
    "developer": "developer",
    "architect": "architect",
    "contractor": "general_contractor",
    "civil_engineer": "civil_engineer",
    "lender": "lender",
    "residential_units": "residential_units",
    "height_ft": "building_height_ft",
    "stories": "num_stories",
    "commercial_gsf": "commercial_gsf",
    "parking_spaces": "parking_spaces",
    "hotel_rooms": "hotel_rooms",
}

NUMERIC = {"residential_units", "height_ft", "stories", "commercial_gsf",
           "parking_spaces", "hotel_rooms"}

# A loan that is not construction financing. The brief is explicit that these
# do not make someone the construction lender.
NOT_CONSTRUCTION = re.compile(
    r"refinanc|acquisition loan|acquisition financing|pre-?development loan|"
    r"pre-?development financing|preferred equity|mezzanine|bridge loan|"
    r"permanent financing|inventory loan", re.I)
# What makes a facility construction financing. "to build" belongs here: a
# sentence like "secured $69.4 million from Deutsche Bank to build a retail
# center" is construction financing as plainly as one that uses the phrase,
# and requiring the literal words rejected several correctly-quoted findings.
#
# "to begin construction" is here for the same reason and is deliberately
# narrow: begin, commence and start only. "to COMPLETE construction" is NOT
# accepted, because a bridge loan for completion and lease-up says exactly
# that -- 222 Namdar's Phase I facility is one -- and accepting it would let
# the NOT_CONSTRUCTION guard be talked out of a correct rejection.
CONSTRUCTION = re.compile(
    r"construction (?:loan|financing|facility|funding|package|debt)|"
    r"c-?pace.{0,40}construction|"
    r"to (?:fund|finance|build|construct|develop)\b|"
    r"to (?:begin|commence|start)\s+construction\b|"
    r"towards? (?:building|construct)|go toward building|"
    r"(?:loan|financing|facility)[^.]{0,60}\bto build\b|"
    r"build(?:ing)? loan", re.I)

# A firm named in one of these roles is not the building's architect.
WRONG_ARCHITECT_ROLE = re.compile(
    r"landscape architect|interior design|interiors by|interior architect", re.I)

SEA_LEVEL = re.compile(r"above sea level|sea level|AMSL", re.I)

# Words that mean a quoted programme DOES contain dwellings, or lodging. A
# stated zero is a real finding -- an office tower genuinely has no residences,
# and leaving the field blank makes that read as unresearched -- but no source
# ever writes the digit 0, so the figure itself cannot be checked against the
# sentence the way every other number is. What can be checked is the sentence:
# a programme enumeration that lists office, retail, restaurant and parking and
# names no dwelling establishes the zero. One that mentions residences does
# not, and is usually a sentence about the OTHER building on the site -- which
# is exactly how the Wynwood Plaza office row would have been given the
# residential building's 509 apartments.
DWELLINGS = re.compile(
    r"residen|apartment|condo|dwelling|home\b|homes\b|unit\b|units\b|"
    r"multifamily|multi-family|townhouse|penthouse|villa", re.I)
LODGING = re.compile(r"hotel|hospitality|\bkeys?\b|\broom\b|\brooms\b|lodging|"
                     r"guestroom|guest room", re.I)
ZERO_CHECKABLE = {"residential_units": DWELLINGS, "hotel_rooms": LODGING}

_WORD = re.compile(r"[A-Za-z0-9]+")

# Storey counts are usually written out, not in digits.
WORD_NUMBERS = {
    1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven",
    8: "eight", 9: "nine", 10: "ten", 11: "eleven", 12: "twelve",
    13: "thirteen", 14: "fourteen", 15: "fifteen", 16: "sixteen",
    17: "seventeen", 18: "eighteen", 19: "nineteen", 20: "twenty",
}


# An above-ground idiom applied to the figure itself. An FAA letter is quoted
# as "a 440-foot-tall structure, or 451 feet above sea level", which states both
# numbers and says which is which -- so requiring the literal words "above
# ground" rejected correctly-quoted above-ground heights. The figure must carry
# the idiom AND the sea-level number must be a different one; if the quoted
# figure is the number attached to "above sea level", it is still refused.
def _above_ground(n, q):
    near = (rf"(?<!\d){n:,}(?!\d)|(?<!\d){n}(?!\d)")
    idiom = re.compile(
        rf"(?:{near})[- ]?(?:foot|feet|ft)?[- ]?(?:tall|high|above ground|above grade)|"
        # "856 feet AGL" is the FAA's own way of saying above ground level, and
        # it is the most explicit form of the statement this guard looks for.
        rf"(?:{near})\s*(?:foot|feet|ft)\.?\s*AGL\b|"
        # "top off at 574-feet" and "tops out at", both common in FAA coverage.
        rf"(?:rise|rises|rising|reaching|stands?|"
        rf"top(?:s|ped|ping)?\s+(?:off|out)\s+at)[^.]{{0,20}}(?:{near})",
        re.I)
    if not idiom.search(q):
        return False
    # The sea-level clause must be about some other figure. The separator is
    # [-\s]* rather than \s* because the same sources write "579-feet above
    # sea level" with a hyphen, and missing that would let an AMSL figure
    # through as though no sea-level clause mentioned it.
    for m in re.finditer(r"([\d,]+)[-\s]*(?:feet|ft\.?)?[-\s]*above (?:mean )?sea level",
                         q, re.I):
        if int(m.group(1).replace(",", "")) == n:
            return False
    return True


def _tokens(value: str):
    stop = {"the", "and", "of", "group", "llc", "inc", "corp", "company", "co",
            "partners", "development", "developments", "architects",
            "architecture", "construction", "builders", "bank", "capital"}
    return [w.lower() for w in _WORD.findall(str(value))
            if len(w) > 2 and w.lower() not in stop]


def check(field, value, quote, url, addends=None):
    """"" if the finding may be stored, else why it may not."""
    if value in (None, "", "not found"):
        return "empty"
    if not url:
        return "no source URL"
    if not quote or len(str(quote).strip()) < 15:
        return "no verbatim quote"
    q = str(quote)

    if field in NUMERIC:
        if int(float(value)) == 0:
            # A stated zero: the quote must be a programme enumeration that
            # names no use of this kind. See DWELLINGS above.
            pat = ZERO_CHECKABLE.get(field)
            if pat is None:
                return "a zero is only a meaningful finding for units or keys"
            hit = pat.search(q)
            if hit:
                return (f"quote mentions {hit.group(0)!r}, so it does not establish "
                        f"that this building has none")
            return ""
        n = int(float(value))
        if addends:
            # A DECLARED SUM. The review's rule is to count every residential
            # unit including condo-hotel units, and sources routinely print the
            # components without a total -- "236 condo-hotel residences ... 163
            # Sky Residences" and no 399 anywhere. Refusing the total made the
            # field blank on exactly the towers it matters most for; accepting
            # a bare total would be the arithmetic this validator exists to
            # stop. So the addends are declared, EACH must appear in the quote,
            # and they must actually add up to the value. The sum is then the
            # source's own figures and only the addition is mine.
            if sum(addends) != n:
                return (f"declared components {addends} total {sum(addends)}, "
                        f"not {n}")
            missing = [a for a in addends
                       if not re.search(rf"(?<!\d){a:,}(?!\d)|(?<!\d){a}(?!\d)", q)]
            if missing:
                return f"component figure(s) {missing} do not appear in the quoted sentence"
            return ""
        # The figure itself must appear in the sentence -- as digits, with or
        # without a thousands separator, or spelled out. Storey counts are
        # routinely written as words ("the six-story medical center", "nine
        # stories above grade"), and requiring digits rejected those as
        # invented when they were quoted correctly.
        forms = [rf"(?<!\d){n:,}(?!\d)", rf"(?<!\d){n}(?!\d)"]
        if n in WORD_NUMBERS:
            forms.append(rf"\b{WORD_NUMBERS[n]}\b")
        if not any(re.search(f, q, re.I) for f in forms):
            return f"the figure {value} does not appear in the quoted sentence"
        if field == "height_ft" and SEA_LEVEL.search(q) and not _above_ground(n, q):
            return "quote mentions sea level; above-ground height not distinguishable"
        return ""

    toks = _tokens(value)
    if field == "name":
        # A project name must be recognisable in the sentence that names it.
        # Half its distinctive words is the bar rather than one, because a
        # single shared word ("Miami", "Residences") is in half the market.
        hits = sum(1 for t in toks if re.search(rf"\b{re.escape(t)}", q, re.I))
        if toks and hits < max(1, len(toks) // 2):
            return f"the quoted sentence does not clearly name {value!r}"
        return ""
    if toks and not any(re.search(rf"\b{re.escape(t)}", q, re.I) for t in toks):
        return f"none of the distinctive words in {value!r} appear in the quoted sentence"

    if field == "lender":
        if NOT_CONSTRUCTION.search(q) and not CONSTRUCTION.search(q):
            return "quote describes a refinancing/acquisition/predevelopment facility, not construction financing"
        if not CONSTRUCTION.search(q):
            return "quote does not say the facility is construction financing"
    if field == "architect" and WRONG_ARCHITECT_ROLE.search(q):
        head = q[:q.lower().find("landscape") if "landscape" in q.lower() else len(q)]
        if not re.search(r"designed by|architect of record|is the architect", head, re.I):
            return "quote names the firm in a landscape or interiors role"
    return ""


def _source_for(sources, field):
    """The source for a field, falling back to the finding's `_default`.

    Most findings rest on one article that states several of the fields, and
    repeating the same URL under eight keys made the files long enough that a
    mismatched URL would be easy to miss. A `_default` entry covers every field
    with no entry of its own; a per-field entry always wins.
    """
    return sources.get(field) or sources.get("_default") or {}


def apply(path, dry_run=False, verbose=True):
    init_db()
    with open(path, encoding="utf-8") as fh:
        findings = json.load(fh)

    db = get_session()
    stored = rejected = 0
    report = []
    # Every value this writes is recorded in the review's change log, so the
    # log cannot drift from the database.
    entries = clog.load()
    try:
        for item in findings:
            row = db.get(Project, int(item["id"]))
            if row is None:
                report.append(("MISSING ROW", item.get("name"), "", "no project with that id"))
                continue
            values = dict(item.get("values") or {})
            sources = item.get("sources") or {}
            quotes = item.get("quotes") or {}

            # A NAME is the most valuable thing research can return for a row
            # the permit feed knows only as an address, so it is applied like
            # any other value -- with a source and a quote, and only onto a row
            # that is still carrying its address as its name.
            new_name = item.get("name")
            if new_name and row.name == row.address:
                name_src = _source_for(sources, "name")
                why = check("name", new_name, quotes.get("name"),
                            name_src.get("url"))
                if why and why != "empty":
                    rejected += 1
                    report.append(("REJECTED", row.name, "name", why))
                elif not why:
                    src = name_src
                    report.append(("NAMED", row.address, "name", new_name))
                    if not dry_run:
                        row.name = new_name
                        db.flush()
                        db.add(ExtractionSource(
                            project_id=row.id, field_name="name",
                            field_value=new_name, filing_name=src.get("name") or "",
                            filing_date=src.get("date") or "", pdf_url=src["url"]))
                    stored += 1
            for field, value in values.items():
                if field not in COLUMNS:
                    report.append(("UNKNOWN FIELD", row.name, field, "not a storable field"))
                    continue
                src = _source_for(sources, field)
                why = check(field, value, quotes.get(field), src.get("url"),
                            (item.get("sum_of") or {}).get(field))
                if why == "empty":
                    continue
                if why:
                    rejected += 1
                    report.append(("REJECTED", row.name, field, why))
                    continue
                existing = getattr(row, COLUMNS[field])
                if existing:
                    # Overwriting is never incidental. A field already holding a
                    # sourced value is replaced only when the finding names it
                    # in `overwrite`, which is how a later filing supersedes an
                    # earlier one -- a scheme cut from 43 storeys to 19, an
                    # architect corrected from the design firm to the firm of
                    # record. The old value is printed so the change is visible.
                    if field not in (item.get("overwrite") or []):
                        report.append(("ALREADY SET", row.name, field,
                                       f"kept {existing!r}, did not overwrite with {value!r}"))
                        continue
                    report.append(("SUPERSEDED", row.name, field,
                                   f"{existing!r} -> {value!r}"))
                clog.record(entries, row.id, row.name, COLUMNS[field],
                            existing, value, src.get("url") or "",
                            item.get("why", {}).get(field, "") if isinstance(
                                item.get("why"), dict) else "")
                if not dry_run:
                    setattr(row, COLUMNS[field], value)
                    db.flush()
                    db.query(ExtractionSource).filter(
                        ExtractionSource.project_id == row.id,
                        ExtractionSource.field_name == COLUMNS[field]).delete(
                            synchronize_session=False)
                    db.add(ExtractionSource(
                        project_id=row.id, field_name=COLUMNS[field],
                        field_value=str(value), filing_name=src.get("name") or "",
                        filing_date=src.get("date") or "", pdf_url=src["url"]))
                stored += 1
                report.append(("STORED", row.name, field, str(value)[:60]))
            if item.get("notes") and not dry_run:
                note = item["notes"].strip()
                cur = (row.notes or "").strip()
                if note and note not in cur:
                    row.notes = f"{cur} {note}".strip() if cur else note
        if not dry_run:
            db.commit()
    finally:
        db.close()
    if not dry_run:
        clog.save(entries)

    if verbose:
        for kind, name, field, detail in report:
            print(f"  {kind:<13} {str(name)[:38]:<38} {field:<18} {detail}")
        print(f"\n{'(dry run) ' if dry_run else ''}{stored} value(s) stored, "
              f"{rejected} rejected by the checks")
    return {"stored": stored, "rejected": rejected, "report": report}


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    apply(a.path, dry_run=a.dry_run)
