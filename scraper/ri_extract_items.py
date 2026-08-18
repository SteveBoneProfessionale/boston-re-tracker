r"""
Extract every field from correctly-segmented Rhode Island agenda items, one pass.

Why this replaces the character-window approach
-----------------------------------------------
The old pipeline anchored on a parcel reference and took +/- 400 characters
around it. Agenda items have no fixed length, so blocks bled into their
neighbours: 94 Moshassuck Street inherited 1 Moshassuck Street's unit count.
Every thin field traced back to that one failure, not to the field patterns.

scraper/ri_segment.py now returns real item boundaries per municipality, so
this module extracts against a block that is actually one project.

Field precedence, highest first:

  1. LABELLED lines. Warwick and Cranston state fields explicitly --
     "Assessor's Plat: 300", "Applicant: Seaview Realty, LLC.",
     "Land Area: 4.11 AC", "Zoning District: B-2". A stated value beats
     anything recovered from prose, so these win outright.
  2. Prose patterns (scraper/ri_extract.py) for the narrative municipalities.
  3. null. A field the filing does not state stays null. Nothing is inferred,
     estimated, or carried over from a neighbouring item.

Writes data/ri_llm_items.json for scraper/ri_ingest_llm.py.

    python scraper/ri_extract_items.py
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import Counter, defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.ri_segment import segment, providence_admin_approvals
from scraper.ri_extract import extract_item
from scraper.ri_sources import BOARDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
CORPUS = ROOT / "data" / "ri_agenda_corpus.json"
TEXT = ROOT / "data" / "ri_pdfs" / "text"
OUT = ROOT / "data" / "ri_llm_items.json"

# ── labelled fields ────────────────────────────────────────────────────────
# Warwick and Cranston print these as a key/value block. The label is the
# filing's own assertion, so it is the most reliable evidence available.
_LABEL = {
    "applicant_entity": r"(?:Owner\s*/\s*Applicant|Applicant\s*/\s*Owner|Applicant|"
                        r"Petitioner|Owner)",
    "zoning_district":  r"(?:Zoning(?:\s+District)?|Existing Zoning)",
    "address":          r"(?:Specific project location|Project location|Location|"
                        r"Property Address|Site Address|Address)",
    "acreage":          r"(?:Land Area|Site Area|Lot Area|Total Area|Acreage)",
    # Curly apostrophe: the PDFs write "Assessor’s", not "Assessor's".
    "plat":             r"(?:Assessor['’]?s?\s+Plat|A\.?P\.?)",
    "lots":             r"(?:Assessor['’]?s?\s+Lots?)",
    "description":      r"(?:Proposal|Project Description|Description|Request)",
    "design_pro":       r"(?:Design professional|Engineer)",
}
# The colon is REQUIRED. With it optional, Cranston's section header "REQUEST
# FOR CONTINUANCE" matched the description label and overwrote the real
# description with "FOR CONTINUANCE". A label without a colon is just prose.
_LABEL_RE = {k: re.compile(rf"^[ \t]*{v}[ \t]*:[ \t]*(.+?)[ \t]*$", re.M | re.I)
             for k, v in _LABEL.items()}

# Narrative municipalities name the applicant as the subject of the sentence:
#   "180 Weeden St LLC seeks Master Plan Approval for ..."
#   "Wood Partners introduces its Preliminary Plan proposal ..."
# Pawtucket's sponsors often carry NO legal suffix, so a suffix-anchored entity
# pattern misses them entirely. Anchor on the verb instead.
_ACTOR = re.compile(
    # A leading street number is part of the name: "180 Weeden St LLC seeks ..."
    r"(?:^|[|.]\s*)((?:\d{1,6}\s+)?[A-Z][A-Za-z0-9&'’.\-]*"
    r"(?:\s+[A-Z0-9][A-Za-z0-9&'’.\-]*){0,5}"
    r"(?:,?\s*(?:LLC|L\.L\.C\.|Inc\.?|Corp\.?|Company|Trust|LP))?)\s+"
    r"(?:seeks|is seeking|proposes|is proposing|requests|is requesting|"
    r"introduces|presents|submits|submitted|applies|has applied)\b")

# Words that are never a sponsor name, however the sentence is shaped.
_NOT_ACTOR = re.compile(
    r"^(?:The|A|An|This|That|Applicant|Petitioner|Owner|Staff|Board|Commission|"
    r"City|Department|Planning|Motion|Request|Proposal|Public|Item|Case|"
    r"Continuation|Workshop|Discussion|Presentation|Vote|Application)\b", re.I)


def actor(block: str) -> str | None:
    """Sponsor named as the acting subject, when the filing states it that way."""
    for m in _ACTOR.finditer(block):
        name = re.sub(r"\s+", " ", m.group(1)).strip(" ,.")
        if len(name) < 3 or _NOT_ACTOR.match(name):
            continue
        return name
    return None

# "4.11 AC total", "0.86-acre", "1.2 acres"
_ACRE_VAL = re.compile(r"([\d,]+(?:\.\d+)?)\s*(?:AC\b|acres?\b|-acre)", re.I)
_INT = re.compile(r"([\d,]{1,9})")

# Parcel citations in every RI dialect:
#   "AP 44 Lot 561"   "AP 62 Lots 291 & 309"   "AP 20/4, Lot 2128"
#   "TAP 6, Lot 1"    "Plat 300 Lots 110, 128"
_PLAT_LOT = re.compile(
    r"\b(?:T?A\.?P\.?|Plat|Assessor'?s?\s+Plat)\s*\.?\s*"
    r"(\d{1,3}(?:/\d{1,3})?)\s*,?\s*"
    r"(?:Lots?|L\.)\s*\.?\s*"
    # Greedy over a digit-anchored list, so it ends on the last LOT NUMBER.
    # A trailing-delimiter lookahead cannot do this: "TAP 6, Lot 1, R-10
    # Residential" needs to stop at 1 (comma follows) while "Lots 110, 128,
    # 247 & 331" must keep going THROUGH the commas. Requiring every
    # continuation to be followed by another number resolves both.
    r"(\d{1,5}(?:\s*(?:,|&|and)\s*\d{1,5})*)", re.I)

# A street address stated inline: "180 Weeden Street", "0 (525) Broadway"
_ADDR = re.compile(
    r"\b(\d{1,6}(?:\s*\(\d{1,6}\))?(?:\s*[-–]\s*\d{1,6})?\s+"
    r"[A-Z][A-Za-z'\.]*(?:\s+[A-Z][A-Za-z'\.]*){0,3}\s+"
    r"(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Way|"
    r"Place|Pl|Court|Ct|Terrace|Ter|Highway|Hwy|Parkway|Pkwy|Circle|Cir|Row|"
    r"Square|Sq|Pike|Trail|Path|Extension|Ext))\b\.?", re.I)

# Named-quotation project titles: Cranston writes  "30 Pomham Street"
_QUOTED = re.compile(r"[\"“”']([^\"“”']{4,70})[\"“”']")


# A second label on the same line ends the first one's value. Cranston writes
# "Owner: RLF IV Terminals SPE, LLC /Applicant: Mineral Enterprises, Inc." on
# one line, which otherwise yields both names glued into a single entity.
_NEXT_LABEL = re.compile(
    r"\s*[/|;]?\s*\b(?:Owner|Applicant|Petitioner|Zoning(?:\s+District)?|"
    r"Proposal|Request|Location|Engineer|Design professional|Ward|"
    r"Assessor['’]?s?\s+(?:Plat|Lots?)|Land Area|Number of Lots)\b\s*:.*$", re.I)


def _clean(v: str | None) -> str | None:
    if not v:
        return None
    v = _NEXT_LABEL.sub("", v)
    v = re.sub(r"\s+", " ", v).strip(" ,.;:-/|")
    # A label whose value ran onto the next label line is not a value.
    if len(v) < 2 or len(v) > 240:
        return None
    return v


def labelled(block: str) -> dict:
    """Fields the filing states under an explicit label."""
    out = {}
    for key, rx in _LABEL_RE.items():
        m = rx.search(block)
        if m:
            out[key] = _clean(m.group(1))
    return {k: v for k, v in out.items() if v}


def parcel_from(block: str) -> tuple[str | None, str | None]:
    """The raw plat/lot citation and its plat number, if the item states one."""
    m = _PLAT_LOT.search(block)
    if not m:
        return None, None
    return m.group(0).strip(), m.group(1)


def address_from(block: str, lab: dict) -> str | None:
    """Street address, preferring a labelled location line."""
    if lab.get("address"):
        a = lab["address"]
        if re.search(r"\d", a) or _ADDR.search(a):
            return a
    m = _ADDR.search(block)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    q = _QUOTED.search(block)
    if q and re.match(r"\s*\d", q.group(1)):
        return q.group(1).strip()
    return None


def build_item(muni: str, seg: dict, meta: dict) -> dict:
    """One agenda item, every field the filing actually states."""
    block = seg["text"]
    base = extract_item(block)
    lab = labelled(block)

    plat_lot_raw, _plat = parcel_from(block)
    # Warwick states plat and lots on separate labelled lines.
    if not plat_lot_raw and lab.get("plat"):
        p = _INT.search(lab["plat"])
        lots = lab.get("lots")
        if p and lots:
            plat_lot_raw = f"AP {p.group(1)} Lot {lots}"

    acreage = base.get("site_acreage")
    if lab.get("acreage"):
        am = _ACRE_VAL.search(lab["acreage"]) or _INT.search(lab["acreage"])
        if am:
            try:
                acreage = float(am.group(1).replace(",", ""))
            except ValueError:
                pass

    # A labelled value outranks a prose guess, per the precedence rule.
    # Precedence: an explicit label, then the subject of the item's own
    # sentence, then a bare entity match. The bare scan goes last because it
    # will happily return any entity anywhere in the block -- an abutter, a
    # prior owner, or a neighbouring item that bled in -- and attributing the
    # wrong sponsor to a project is worse than leaving it null.
    applicant = (lab.get("applicant_entity") or actor(block)
                 or base.get("applicant_entity"))
    if applicant and re.match(r"^(the\s+)?applicant\b", applicant, re.I):
        applicant = None            # "Applicant: The applicant proposes..." is not a name
    zoning = lab.get("zoning_district") or base.get("zoning_district_raw")

    stages = base.get("review_stages") or []
    item = {
        "municipality": muni,
        "meeting_date": meta["date"],
        "reviewing_body": meta["board"],
        "entity_id": meta["entity_id"],
        "source_url": meta["source_url"],
        "document": meta["document"],

        "address": address_from(block, lab),
        "plat_lot_raw": plat_lot_raw,
        "applicant_entity": applicant,
        "case_number": base.get("case_number") or seg.get("case_number"),
        "zoning_district": zoning,
        "residential_units": base.get("residential_units"),
        "parking_spaces": base.get("parking_spaces"),
        "square_feet": base.get("total_gsf"),
        "acreage": acreage,
        "stories": base.get("num_stories"),
        "building_count": base.get("building_count"),
        "adaptive_reuse": base.get("adaptive_reuse"),
        "classification": base.get("classification") or seg.get("section"),
        "neighborhood": base.get("neighborhood"),
        "description": lab.get("description") or base.get("description"),
        "review_stage": stages[0] if stages else None,
        "review_stage_raw": base.get("review_stage_raw") or seg.get("section"),
        "vote_taken": base.get("vote_taken"),
        "outcome": base.get("outcome"),
        "advances_stage": base.get("advances_stage", True),
        "administrative": False,
    }

    # Pawtucket prints the vote marker on the item's own header line.
    if seg.get("vote_marker"):
        item["vote_taken"] = seg["vote_marker"].upper() == "VOTE TAKEN"
    if seg.get("address_line"):
        pl, _ = parcel_from(seg["address_line"])
        if pl:
            item["plat_lot_raw"] = pl
        a = _ADDR.search(seg["address_line"])
        if a:
            item["address"] = re.sub(r"\s+", " ", a.group(1)).strip()
    return item


def run() -> dict:
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    boards = {b["entity_id"]: b["name"] for b in BOARDS}

    items: list[dict] = []
    admin_count = 0
    per_doc = Counter()

    for v in corpus.values():
        muni = v["municipality"]
        eid = v.get("entity_id")
        for d in v["documents"]:
            p = TEXT / d["text_file"]
            if not p.exists():
                continue
            text = p.read_text(encoding="utf-8", errors="replace")
            if len(text) < 800:
                continue
            meta = {
                "date": v["date"],
                "board": boards.get(eid, v.get("board") or ""),
                "entity_id": eid,
                "source_url": d.get("url", ""),
                "document": d["text_file"],
            }
            for seg in segment(muni, text, eid):
                items.append(build_item(muni, seg, meta))
            per_doc[muni] += 1

            # Providence administrative approvals are recorded but flagged so
            # they never inflate the development pipeline.
            if muni == "Providence":
                for a in providence_admin_approvals(text):
                    pl, _ = parcel_from(a["description"])
                    am = _ADDR.search(a["description"])
                    items.append({
                        "municipality": muni, "meeting_date": v["date"],
                        "reviewing_body": meta["board"], "entity_id": eid,
                        "source_url": meta["source_url"], "document": d["text_file"],
                        "address": am.group(1).strip() if am else None,
                        "plat_lot_raw": pl,
                        "applicant_entity": None,
                        "case_number": a["case_number"],
                        "description": a["description"],
                        "review_stage": "Administrative Review",
                        "review_stage_raw": "Administrative approval",
                        "advances_stage": True, "administrative": True,
                        "zoning_district": None, "residential_units": None,
                        "parking_spaces": None, "square_feet": None,
                        "acreage": None, "stories": None, "building_count": None,
                        "adaptive_reuse": False, "classification": None,
                        "neighborhood": None, "vote_taken": None, "outcome": None,
                    })
                    admin_count += 1

    # Drop items with no identity at all -- nothing to key a project on.
    keyed = [i for i in items if i.get("plat_lot_raw") or i.get("address")]

    OUT.write_text(json.dumps(keyed, indent=1), encoding="utf-8")

    fields = ["applicant_entity", "residential_units", "square_feet",
              "parking_spaces", "acreage", "zoning_district", "plat_lot_raw",
              "address", "review_stage", "vote_taken", "description"]
    cov = defaultdict(lambda: defaultdict(int))
    tot = Counter()
    for i in keyed:
        m = i["municipality"]
        tot[m] += 1
        for f in fields:
            if i.get(f) not in (None, "", False, []):
                cov[m][f] += 1

    log.info("Documents read: %d   items segmented: %d   with identity: %d   "
             "(admin-flagged: %d)", sum(per_doc.values()), len(items), len(keyed),
             admin_count)
    munis = sorted(tot)
    log.info("")
    log.info("%-22s%s", "ITEM COVERAGE", "".join(f"{m[:9]:>11}" for m in munis))
    log.info("%-22s%s", "  items", "".join(f"{tot[m]:>11}" for m in munis))
    for f in fields:
        log.info("%-22s%s", "  " + f,
                 "".join(f"{100*cov[m][f]//max(tot[m],1):>10}%" for m in munis))
    log.info("\nWrote %s", OUT)
    return {"items": len(keyed), "admin": admin_count}


if __name__ == "__main__":
    run()
