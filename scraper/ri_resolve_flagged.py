r"""
Resolve the records the scope pass left flagged.

The first pass classified 181 KEEP, 17 REMOVE and 106 FLAG. A third of the
Rhode Island set sitting undecided is not a usable state, so this pass reads
each flagged record again -- full scoped text plus the structured extraction --
and forces a decision where the evidence supports one.

WHAT MADE THEM UNDECIDABLE THE FIRST TIME. Almost all of them fail on the same
two tests: no construction language and no programme. That is usually true of
the TEXT rather than of the PROJECT, because a zoning-board agenda states the
relief sought and not the building behind it. So the resolution here comes
mostly from reading what the filing is FOR, not from finding a missing number.

ORDER OF DECISION, first rule that fires wins:

  1. PROTECTED    a Major, Minor or Administrative land development
                  classification, or an established completion, is positive
                  evidence by itself. Under RIGL 45-23 a "land development
                  project" IS a development; the agenda does not have to
                  restate it. Kept, never removed.
  2. BUILDS       the filing describes making something -- construct, erect,
                  adaptive reuse, conversion, addition, N units, N rooms.
  3. REZONING     kept as a distinct pipeline ENTRY POINT, per the standing
                  decision on rezonings. A map amendment is the first move of
                  a development even when no building is described yet.
  4. NOT A PROJECT the filing is relief, paperwork or an operational change:
                  sign illumination, a parking-space count, a change of use
                  with no construction, procedural boilerplate, a variance on
                  a one- or two-family house, a subdivision creating bare lots.
  5. otherwise    still undecidable. Left flagged. Not forced.

    python scraper/ri_resolve_flagged.py
    python scraper/ri_resolve_flagged.py --apply
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import text_index, project_text, _full_text, RI
from scraper.ri_identity import normalize_address
from scraper.ri_commercial import COMMERCIAL_USE, SMALL_RESI, UNIT_FLOOR

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
OUT = ROOT / "data" / "ri_flag_resolution.json"

# 2 -- the filing describes making something.
BUILDS = re.compile(
    r"\b(?:to\s+)?construct\w*\b|\berect\w*\b|\badaptive\s+reuse\b|\bconversion\s+of\b|"
    r"\bconvert\w*\s+(?:the\s+)?(?:existing\s+)?\w+\s+(?:to|into)\b|"
    r"\bnew\s+(?:building|structure|construction|principal\s+structure)\b|"
    r"\baddition\s+(?:to|of)\b|\bredevelop\w*\b|\bsubstantial\s+rehabilitation\b|"
    r"\b\d+\s*(?:dwelling\s+)?units?\b|\b\d+\s*(?:guest|bed)\s*rooms?\b|"
    r"\b\d+[\s-]stor(?:y|ies|ey)\b", re.I)

REZONING = re.compile(r"\brezon\w+\b|\bzone\s+change\b|\bmap\s+amendment\b|"
                      r"\brezoning\s+of\b|\bfrom\s+R-\d\w*\s+to\s+[A-Z]", re.I)

# 4 -- relief, paperwork, or an operational change. Each of these was read off
# an actual flagged record rather than imagined.
NOT_A_PROJECT = [
    ("sign or illumination relief", re.compile(
        r"\bsign\s+illumination\b|\billumination\s+standards\b|\bsignage?\b[^.]{0,50}"
        r"\b(?:waiver|relief|height|variance)\b|\btemporary\s+sign\b", re.I)),
    ("a parking-space count, not a building", re.compile(
        r"\b(?:additional|extra)\s+\d*\s*(?:tractor\s+trailer\s+)?parking\s+spaces?\b|"
        r"\bparking\s+(?:modification|space\s+count|relief)\b|\brestrip\w*", re.I)),
    ("change of use with no construction", re.compile(
        r"\bchange\s+of\s+use\b(?![^.]{0,80}\b(?:construct|renovat|convert|addition|"
        r"adaptive)\b)|\bto\s+establish\s+a\b[^.]{0,60}\bin\s+an?\s+existing\b", re.I)),
    ("procedural text, not a filing about a building", re.compile(
        r"\brecommendations?\s+to\s+the\s+zoning\s+board\b|\bwill\s+take\s+a\s+single\s+vote\b|"
        r"\bincorporated\s+the\s+previously\s+granted\s+waivers?\b|"
        r"\bwas\s+present\s+and\s+representing\b|\bmeeting\s+minutes\b\s*[-–]|"
        r"\bcomprehensive\s+plan\s+pertaining\s+to\b|\bpolicy\s+ED-\d+\b", re.I)),
    ("relief on a one- or two-family house", re.compile(
        r"\b(?:use|dimensional)\s+variance\b[^.]{0,200}\b(?:single|one|two)[- ]family\b|"
        r"\b(?:single|one|two)[- ]family\b[^.]{0,200}\b(?:use|dimensional)\s+variance\b", re.I)),
    ("a subdivision creating bare lots with no building programme", re.compile(
        r"\bsubdivision\s+will\s+create\b[^.]{0,80}\blots?\b|"
        r"\bcreate\s+\w+\s*\(?\d*\)?\s*(?:non-?conforming\s+)?lots?\s+from\b", re.I)),
    ("an extension or renewal of an existing approval", re.compile(
        r"\brequests?\s+that\s+(?:resolution|decision)[^.]{0,60}\bbe\s+extended\b|"
        r"\bextension\s+of\s+(?:the\s+)?(?:time|approval|resolution|decision)\b|"
        r"\brenew\w*\s+(?:the\s+)?(?:approval|resolution|permit)\b|"
        r"\breinstatement\s+of\s+(?:the\s+)?(?:approval|decision)\b", re.I)),
    ("an interior alteration or a use expanding inside an existing building", re.compile(
        r"\binterior\s+alterations?\b|"
        r"\bexpan\w+\s+(?:the\s+)?existing\s+\w+\s+(?:office|use|business)\b|"
        r"\bexpansion\s+of\s+(?:the\s+)?\w*\s*\w+\s+use\b[^.]{0,60}\binto\s+the\b|"
        r"\bexpand\w*\s+the\s+existing\s+use\s+by\s+variance\b", re.I)),
    ("a change of use, stated as establishing a use on the property", re.compile(
        r"\bestablish\s+the\s+use\s+of\s+the\s+propert\w+\s+as\b|"
        r"\bproposes?\s+to\s+establish\s+(?:the\s+)?use\b", re.I)),
    ("a storefront or facade replacement", re.compile(
        r"\breplace\s+an?\s+existing\b[^.]{0,40}\bstorefront\b|\bfacade\s+(?:work|"
        r"alteration|replacement)\b|\bstorefront\s+(?:replacement|alteration)\b", re.I)),
    ("a rooftop, deck or patio alteration", re.compile(
        r"\brooftop\s+(?:penthouse|patio|deck|terrace)\b|\broof\s+deck\b|"
        r"\boutdoor\s+(?:seating|patio)\b", re.I)),
]


def anchored_text(p, idx, span=700):
    """The project's own item, read from the UNTRUNCATED document.

    The scope window is tuned to stop one item's figures leaking into its
    neighbour, and it does that well. But a Providence zoning agenda states
    the applicant and the address first and the actual request a sentence or
    two later, so the window frequently cut off at "Application for USE
    VARIAN..." -- exactly where the answer starts. Every one of the 37
    records this pass could not decide failed that way; none of them was
    missing from the source. Anchoring on the address and reading forward
    recovers the request without widening the window for everyone.
    """
    items = idx.get((p.city.lower(), normalize_address(p.address or "")), [])
    addr = (p.address or "").strip()
    if not addr:
        return ""
    head = re.escape(addr[:16]).replace(r"\ ", r"\s+")
    out = []
    for it in items:
        ft = _full_text(it.get("document") or "")
        if not ft:
            continue
        for m in re.finditer(head, ft, re.I):
            out.append(ft[m.start():m.start() + span])
    return "\n".join(dict.fromkeys(out))


def resolve(p, text):
    """(verdict, reason, sentence)."""
    def sent(rx):
        m = rx.search(text)
        if not m:
            return ""
        i = max(0, m.start() - 80)
        return re.sub(r"\s+", " ", text[i:m.end() + 150]).strip()

    if p.review_scale in ("Major", "Minor", "Administrative") or p.completion_stage:
        return ("KEEP", "carries a %s land development classification, which is itself the "
                "finding that this is a development"
                % (p.review_scale or "completed"), "")

    units = p.residential_units or 0
    if units >= UNIT_FLOOR and not SMALL_RESI.search(text):
        return "KEEP", "%d residential units, at or above the %d-unit floor" % (units, UNIT_FLOOR), ""

    # A filing that describes making something is a project, even when the
    # agenda never states a size.
    if BUILDS.search(text):
        # ...unless what it makes is bare lots.
        for why, rx in NOT_A_PROJECT:
            if "subdivision" in why and rx.search(text):
                return "REMOVE", why, sent(rx)
        return "KEEP", "the filing describes construction, conversion or a stated programme", sent(BUILDS)

    if REZONING.search(text):
        return ("KEEP", "a rezoning, kept as a distinct pipeline entry point rather than a "
                "normal project", sent(REZONING))

    for why, rx in NOT_A_PROJECT:
        if rx.search(text):
            return "REMOVE", why, sent(rx)

    if SMALL_RESI.search(text) and not COMMERCIAL_USE.search(text):
        return "REMOVE", "single or two-family residential", sent(SMALL_RESI)

    return "UNDECIDED", "no construction, no programme, no commercial use, and no filing type that settles it", \
        re.sub(r"\s+", " ", text[:240]).strip()


def main(apply=False):
    idx = text_index()
    session = get_session()
    fl = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
          if not p.excluded and p.is_flagged]
    # Largest first, so a partial run resolves the ones that matter.
    fl.sort(key=lambda p: (-(p.residential_units or 0), -(p.total_gsf or 0),
                           0 if p.developer else 1))

    out = []
    for p in fl:
        text, _ = project_text(p, idx)
        text = text or (p.description or "")
        v, why, sentence = resolve(p, text)
        # Only the undecided are re-read from the untruncated document, so a
        # record already settled on its own item text is never re-opened
        # against a wider window that might catch a neighbour's words.
        if v == "UNDECIDED":
            wide = anchored_text(p, idx)
            if wide:
                v2, why2, sent2 = resolve(p, wide)
                if v2 != "UNDECIDED":
                    v, why, sentence = v2, why2 + " (read from the untruncated document)", sent2
        out.append({"id": p.id, "city": p.city, "address": (p.address or p.name or "")[:46],
                    "units": p.residential_units, "sf": p.total_gsf,
                    "developer": (p.developer or "")[:30], "scale": p.review_scale,
                    "verdict": v, "reason": why, "sentence": sentence[:280]})

    t = Counter(r["verdict"] for r in out)
    log.info("\nRESOLVING %d FLAGGED RECORDS\n", len(fl))
    log.info("  KEEP      %4d", t.get("KEEP", 0))
    log.info("  REMOVE    %4d", t.get("REMOVE", 0))
    log.info("  UNDECIDED %4d  (left flagged, not forced)", t.get("UNDECIDED", 0))

    log.info("\nBY CITY")
    for c in sorted({r["city"] for r in out}):
        cc = Counter(r["verdict"] for r in out if r["city"] == c)
        log.info("  %-11s keep %3d  remove %3d  undecided %3d", c,
                 cc.get("KEEP", 0), cc.get("REMOVE", 0), cc.get("UNDECIDED", 0))

    log.info("\nKEEP REASONS")
    for why, n in Counter(r["reason"] for r in out if r["verdict"] == "KEEP").most_common():
        log.info("  %4d  %s", n, why[:88])
    log.info("\nREMOVE REASONS")
    for why, n in Counter(r["reason"] for r in out if r["verdict"] == "REMOVE").most_common():
        log.info("  %4d  %s", n, why[:88])

    risky = [r for r in out if r["verdict"] == "REMOVE"
             and ((r["units"] or 0) >= 5 or (r["sf"] or 0) > 0)]
    log.info("\nREMOVALS STATING 5+ UNITS OR ANY FLOOR AREA: %d", len(risky))
    for r in risky:
        log.info("  id=%-4d %-11s u=%-4s sf=%-8s %-34s %s", r["id"], r["city"], r["units"],
                 r["sf"], r["address"][:34], r["reason"][:40])

    und = [r for r in out if r["verdict"] == "UNDECIDED"]
    if und:
        log.info("\nSTILL UNDECIDABLE (%d):", len(und))
        for r in und:
            log.info("  id=%-4d %-11s %-34s", r["id"], r["city"], r["address"][:34])
            log.info("        %s", r["sentence"][:150])

    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False), encoding="utf-8")

    if apply:
        for r in out:
            p = session.get(Project, r["id"])
            if r["verdict"] == "REMOVE":
                p.excluded = True
                p.excluded_reason = r["reason"]
                p.is_flagged = False
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "QUARANTINED on flag review: %s. Justifying text: %r"
                    % (r["reason"], r["sentence"][:200]))
            elif r["verdict"] == "KEEP":
                p.is_flagged = False
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "SCOPE FLAG CLEARED: %s" % r["reason"])
        session.commit()
        log.info("\nAPPLIED")
    else:
        log.info("\nDRY RUN -- re-run with --apply")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
