r"""
STEP 1 of the two-step developer method for Rhode Island.

Read the planning-board documents already on disk for each project -- every
agenda and minutes item that collapsed into it, not just the one description
stored on the row -- and take any company name appearing ANYWHERE in that text
as a candidate. Labeled fields ("Applicant:", "Petitioner:") are a source of
candidates, not a restriction: a company named only in narrative prose counts
exactly the same.

    "PRESENTATION BY CHURCHILL AND BANKS REGARDING A PROPOSED DEVELOPMENT"

carries the developer as plainly as any labeled field, and the earlier
label-only extractor missed it.

Writes data/ri_developer_candidates.json for step 2 to verify. Writes nothing
to the database -- a candidate is not an answer.

    python scraper/ri_developer_candidates.py
"""

import re
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project
from scraper.ri_identity import normalize_address
from scraper.ri_ingest_llm import load_items, collapse
from scraper.ri_shell import is_shell_name, has_legal_suffix, STREETS, _STREET_TYPE

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

OUT = Path(__file__).parent.parent / "data" / "ri_developer_candidates.json"

# Digits and "+" belong inside a name: D+P Real Estate, E2000 LLC, AE128
# Wayland LLC. Excluding them silently dropped every such company.
CAP = r"[A-Z][A-Za-z0-9&+'.\-]*"
# A name can carry a number in the MIDDLE, not just at the front: "L&G 150
# Richmond Holdings LLC". Requiring every token to start with a capital cut
# such names off at the number, and the truncated stub then lost the ranking
# to whatever unrelated entity the same page happened to mention.
TOK = r"(?:" + CAP + r"|\d[A-Za-z0-9\-]*)"
SEQ = CAP + r"(?:\s+(?:of|and|the|de|at|on|&)\s+" + TOK + r"|\s+" + TOK + r"){0,5}"

# A. Labeled roles. Plural and singular, and the narrative equivalents.
LABELED = re.compile(
    r"(?:Applicant|Petitioner|Proponent|Owner|Developer|Sponsor|Contract\s+Purchaser)s?"
    r"\s*(?:/\s*Owner)?\s*[:\-]\s*(?P<n>.{3,90}?)"
    r"(?=\s{2,}|\s+(?:The|This|is|are|has|have|seeks?|requests?|proposes?)\s|[;.\n]|$)",
    re.I)
NARRATIVE = re.compile(
    r"(?:Proposal\s+by|Application\s+of|Petition\s+of|Request\s+of|Presentation\s+by|"
    r"submitted\s+by|filed\s+by|on\s+behalf\s+of|representing)\s+(?:the\s+)?"
    r"(?P<n>" + SEQ + r")", re.I)

# B. Any entity carrying a legal suffix, anywhere in the prose.
LEGAL = re.compile(
    r"(?P<n>" + SEQ + r",?\s+"
    r"(?:LLC|L\.L\.C\.|Inc|Inc\.|Corp|Corp\.|Corporation|Company|Co\.|"
    r"LP|L\.P\.|LLP|Ltd|Ltd\.|Trust|Realty|Associates|Partners|Properties))\b")

# C. Developer-shaped nouns, anywhere, even with no legal suffix.
DEVWORD = re.compile(
    r"(?P<n>" + SEQ + r"\s+"
    r"(?:Development|Developments|Developers|Properties|Realty|Associates|Partners|"
    r"Partnership|Group|Enterprises|Holdings|Builders|Construction|Capital|Ventures|"
    r"Communities|Homes|Housing\s+Authority|Redevelopment\s+Agency|University|College))\b"
    # "the District Development Plan" is a zoning instrument, not a company.
    # The capture stops before "Plan", so the reject list never sees the word
    # that gives it away -- the guard has to look at what follows the match.
    r"(?!\s+(?:Plan|Plans|Ordinance|Regulations|Standards|Guidelines|Act|"
    r"District|Area|Zone|Overlay|Review|Process|Agreement|Fee|Fees))")

# D. Shouted names in minutes text: "PRESENTATION BY CHURCHILL AND BANKS".
SHOUT = re.compile(
    r"\b(?P<n>[A-Z][A-Z&.\-]{2,}(?:\s+(?:AND|&|OF|THE)\s+[A-Z][A-Z&.\-]{2,}"
    r"|\s+[A-Z][A-Z&.\-]{2,}){1,4})\b")

# Not developers. Boards, staff, professions and procedural boilerplate.
# NOTE: every noun here takes an explicit `s?` -- the trailing \b belongs to the
# whole alternation, so a bare "petitioner" silently failed to match
# "petitioners" and the entire reject list leaked on plural forms.
REJECT = re.compile(
    r"\b(?:city\s+plan|plan\s+commissions?|zoning\s+boards?|planning\s+boards?|"
    r"city\s+councils?|board\s+of\s+review|"
    # "department" alone also rejected genuine agency applicants such as a
    # state Department of Education. Only the reviewing bodies belong here.
    r"planning\s+departments?|building\s+departments?|zoning\s+departments?|"
    r"fire\s+departments?|police\s+departments?|departments?\s+of\s+public\s+works|"
    r"divisions?|commissions?|"
    r"committees?|clerks?|esq|attorneys?|law\s+offices?|counsel|architects?|"
    r"architecture|engineering|engineers?|surveyors?|landscape|consultants?|traffic|"
    r"abutters?|staff|applicants?|petitioners?|proponents?|public\s+hearings?|"
    r"meetings?|minutes|agendas?|motions?|seconded?|votes?|roll\s+call|ayes?|nays?|"
    r"chairman|chairperson|chair|vice|members?|directors?|"
    r"rigl|general\s+laws|ordinances?|sections?|articles?|comprehensive\s+plan|"
    r"future\s+land\s+use|assessors?|present\s+and\s+representing|recommendations?|"
    r"approvals?|denials?|continued|withdrawn|stipulations?|appearances?|"
    r"respectfully|discussion|testimony|deliberation|"
    r"(?:major|minor|unified)\s+land\s+development|land\s+development\s+projects?|"
    r"development\s+agreements?|development\s+plans?|granted|denied|"
    r"(?:old|new)\s+business|adjournment|call\s+to\s+order|city\s+hall|"
    r"council\s+chambers|major\s+changes?|public\s+informational|pre-?applications?|"
    r"unified\s+development\s+review|development\s+projects?|dwelling\s+units?|"
    r"sign\s+illumination|must\s+notify|office\s+of\s+neighbou?rhood|"
    r"illumination\s+standards|recent\s+activities|initiatives|intiatives|"
    # Providence posts its agendas in Spanish as well, so the same procedural
    # headings recur in translation and were being read as company names.
    r"asuntos?\s+continuados?|nuevos?\s+asuntos?|audiencia\s+p[uú]blica|"
    r"solicitantes?|propietarios?|peticionarios?|reuni[oó]n|comisi[oó]n|"
    r"varianzas?|variaci[oó]n|permisos?|uso\s+especial|junta\s+de|"
    r"working\s+group|comprehensive\s+plans?|\d{4}\s+plan|"
    # Council ward labels. Allowing interior numerals let "WARD 2" glue itself
    # to the front of the applicant that followed it on the agenda line.
    r"wards?\s+\d{1,2}|page\s+\d+\s+of\s+\d+|"
    # "Asuntos Nuevos" appears in either word order on the bilingual agendas.
    r"asuntos?\s+nuevos?|new\s+matters?|old\s+matters?|important\s+information|other\s+accommodations?|(?:minor|major|administrative)\s+subdivisions?|other\s+bus[il]bess|other\s+business|zoning\s+districts?|publicly\s+held\s+property|individuals?\s+requiring|main\s+street\b(?!\s+\w)|\bzone\b\s*$|public\s+informa|advanced?\s+public|one\s+week)\b",
    re.I)

# Generic phrases that are procedure or legal form, not a company.
BOILERPLATE = re.compile(
    r"^(?:new\s+construction|(?:major|minor|unified)?\s*land\s+development(?:\s+"
    r"(?:project|review|plan))?|development\s+(?:agreement|project|plan|review|district)|"
    r"housing\s+and\s+urban\s+development|redevelopment\s+district|"
    r"(?:delaware|rhode\s+island|domestic|foreign)?\s*limited\s+"
    r"(?:partnership|liability\s+company)|limited\s+partnership|"
    r"comprehensive\s+permit|special\s+use\s+permit|use\s+variance|"
    r"dimensional\s+variance|building\s+permit|certificate\s+of\s+occupancy|"
    r"regular\s+hearing|site\s+plan|master\s+plan|preliminary\s+plan|final\s+plan|"
    r"amended\s+and\s+restated|first\s+amendment|subject\s+property|"
    r"existing\s+building|proposed\s+building|purpose\s+of\s+the\s+\w+)$", re.I)

# A name ends where the sentence turns back into prose.
STOP_AT = re.compile(
    r"^(?:regarding|concerning|requesting|requests?|proposing|proposals?|seeking|"
    r"seeks?|located|presented|presentation|discussion|possible|located|for|to|re|"
    r"is|was|were|will|has|have|had|been|being|that|which|who|whose|whom|at|in|on|"
    r"by|with|from|about|per|via|pursuant|under|before|after|during)$", re.I)

# re.I matters: without it "The" (capitalised, as it appears mid-sentence)
# never matched, so trailing joining words survived into the name.
CONNECTOR = re.compile(r"^(?:of|and|the|de|at|on|&|for)$", re.I)
LEGALWORD = re.compile(r"^(?:LLC|L\.L\.C\.|Inc\.?|Corp\.?|Corporation|Company|Co\.?|"
                       r"LP|L\.P\.|LLP|Ltd\.?|Trust|Realty|Associates|Partners|"
                       r"Properties|Partnership|Group|Enterprises|Holdings)$", re.I)

STOPWORD_ONLY = re.compile(r"^(?:the|this|that|a|an|and|of|for|to|is|are|it|no|new|"
                           r"said|subject|property|site|building|project|lot|lots|"
                           r"street|avenue|road|drive|place|court)$", re.I)


# Abbreviations whose full stop does NOT end a sentence. Without this the
# sentence-splitter below would cut "H.V. Collins" down to "Collins".
# (?:[A-Z]\.)*[A-Z] covers a run of initials: the token before the stop in
# "H.V. Collins" is "H.V", not a single letter, so a bare [A-Z] test cut the
# company down to "Collins".
_ABBREV = re.compile(r"^(?:(?:[A-Z]\.)*[A-Z]|Inc|Corp|Co|Ltd|Assoc|Bros|St|Ave|Rd|"
                     r"Blvd|Mr|Mrs|Ms|Dr|Jr|Sr|Esq|No|Ste|Apt|Dept)$", re.I)
ROLE_SUFFIX = re.compile(
    r"\s*,?\s*(?:Esq|Directors?|Administrators?|Officers?|Clerks?|Secretary|"
    r"Treasurer|Solicitors?|Engineers?|Architects?|AICP|P\.?E\.?|RLA|"
    r"Chair(?:man|person|woman)?)\b", re.I)
_HONORIFIC = re.compile(r"^(?:Mr|Mrs|Ms|Miss|Dr|Atty|Attorney|Hon)\.?\s+", re.I)


def _split_sentences(n):
    """Keep the last sentence-like segment: prose often precedes the entity.

    "City Forester. WATERMAN AND IVES REALTY, LLC" -> the company. A full stop
    only counts as a break when the word before it is a real word rather than
    an initial or a known abbreviation.
    """
    parts, buf = [], []
    tokens = n.split(" ")
    for i, tok in enumerate(tokens):
        buf.append(tok)
        if tok.endswith(".") and not _ABBREV.match(tok[:-1]) and i < len(tokens) - 1:
            parts.append(" ".join(buf))
            buf = []
    if buf:
        parts.append(" ".join(buf))
    if not parts:
        return n, ""
    return parts[-1].strip(), " ".join(parts[:-1]).strip()


def _clean(name):
    n = re.sub(r"\s+", " ", (name or "")).strip(" ,;:.-–—")
    n = re.sub(r"^(?:the|and)\s+", "", n, flags=re.I)
    # "Providence Community Health Centers (PCHC)" -- the bracketed initialism
    # made the last token start with "(", which failed the capitalisation test
    # and threw the whole name away.
    n = re.sub(r"\s*\([^)]*\)\s*$", "", n).strip()
    n, dropped = _split_sentences(n)
    # If the discarded prefix is procedural prose, the capture ran across a
    # sentence and whatever survives is incidental, not the applicant:
    #   "representing the petitioners. Daniel Geagan from the Planning Dept"
    # leaves the staff member who read the recommendations into the record.
    # Testing the dropped text rather than the whole capture keeps genuine
    # names that merely contain a listed word ("... Department of Education").
    if dropped and REJECT.search(dropped):
        return None
    n = _HONORIFIC.sub("", n).strip()

    # Truncate where the capture ran on into prose ("Churchill and Banks
    # REGARDING a proposed development" -> "Churchill and Banks").
    words = [w for w in re.split(r"\s+", n) if w]
    cut = []
    for w in words:
        if STOP_AT.match(w.strip(",.;:")):
            break
        cut.append(w)
    n = " ".join(cut).strip(" ,;:.-–—")
    if not n:
        return None

    # Drop a trailing connector left behind by the truncation.
    while cut and CONNECTOR.match(cut[-1].strip(",.;:")):
        cut.pop()
        n = " ".join(cut).strip(" ,;:.-–—")
    if len(n) < 4 or len(n) > 70:
        return None
    if REJECT.search(n) or BOILERPLATE.match(n):
        return None
    # A mailing address is not a company: "3315 N OAK TRFY KANSAS CITY, MO
    # 64111". A US state abbreviation followed by a ZIP, or a postal street
    # abbreviation, gives it away.
    if re.search(r"\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b", n):
        return None
    # Postal street abbreviations mark a mailing address wherever they sit --
    # requiring a leading digit missed "Oak Trfy Kansas City", the same
    # address arriving through the shouted-text tier in title case.
    if re.search(r"\b(?:TRFY|HWY|PKWY|TPKE|BLVD)\b", n, re.I):
        return None

    words = [w for w in re.split(r"[\s,]+", n) if w]
    # Ten, not six or eight: a labelled field routinely names a joint
    # applicant, and two full LLC names plus "and" runs long -- "21 Peace
    # Street LLC and Urban Land Development LLC" is 9 words, and losing it
    # left the former St Joseph Hospital rezoning with no developer at all.
    # The labelled capture is already bounded at 90 characters upstream.
    if not 1 <= len(words) <= 10:
        return None
    # A name cannot START with a legal form. "LLC Construction" is a capture
    # that ran across the end of one entity into the next sentence:
    #   "Applicant: Champlin Heights II, LLC | Construction of a solar array"
    if LEGALWORD.match(words[0].strip(",.;:")):
        return None
    if all(STOPWORD_ONLY.match(w) for w in words):
        return None
    # OCR run-together text ("thepetitioners DanielGeagan fromthePlanning...").
    if any(len(w) > 24 for w in words):
        return None
    # Every token must read as part of a name: capitalised, a legal form, or a
    # connector. A stray lowercase verb means the capture swallowed prose.
    for w in words:
        t = w.strip(",.;:&")
        if not t:
            continue
        if CONNECTOR.match(t) or LEGALWORD.match(t):
            continue
        # A street number leads a great many real entity names ("27 E River
        # LLC", "1145 Main Associates"). str.isupper() is False for a digit,
        # so testing t[0].isupper() alone threw all of them away.
        if t[0].isdigit():
            continue
        if not t[0].isupper():
            return None
    # A lone word is only a name if it carries real length and no generic
    # sense. Four, not six: "AS220" is five characters and is a real Providence
    # arts organisation that was being dropped by the longer minimum.
    if len(words) == 1 and (len(n) < 4 or LEGALWORD.match(words[0])):
        return None
    # Two letters, not three: "AS220" has only "AS" as a consecutive run and
    # was failing this test outright.
    if not re.search(r"[A-Za-z]{2}", n):
        return None
    # A bare street name is not a company. "Post Road" came off the Warwick
    # city clerk's letterhead ("3275 POST ROAD") once the planning director's
    # name was correctly rejected. A legal suffix rescues the real entities
    # that are named for their street, e.g. "Post Road, LLC".
    _w = [x.strip(",.") for x in n.split()]
    if (len(_w) <= 3 and not has_legal_suffix(n)
            and any(_STREET_TYPE.match(x) for x in _w)
            and " ".join(_w[:-1]).upper() in STREETS):
        return None
    # "45 Parade LLC 5" -- a stray trailing numeral from the scanned page.
    n = re.sub(r"\s+\d{1,2}$", "", n).strip()
    # The name is present twice in the assembled text (description plus the
    # applicant field) and one capture spanned both copies -- exactly
    # ("Dunkin Donuts Dunkin Donuts"), partially ("Francisca Polanco Navedo
    # Francisca Polanco"), or with a joining word ("Narragansett Electric
    # Company The Narragansett Electric Company"). Collapse any tail that
    # merely repeats the head.
    ws = n.split()
    for i in range(1, len(ws)):
        tail = ws[i:]
        if tail and tail == ws[:len(tail)]:
            ws = ws[:i]
            break
    while ws and CONNECTOR.match(ws[-1].strip(",.;:")):
        ws.pop()
    n = " ".join(ws).strip(" ,;:.-")
    return n or None


def _strip_address_prefix(name, text, start):
    """Drop a street name welded onto the front of an entity.

    "Newport City Hall 43 Broadway Hillsgrove Homes, LLC" is an address
    running straight into the next entity with no punctuation between them.
    The capture takes "Broadway Hillsgrove Homes, LLC"; the company is
    "Hillsgrove Homes, LLC". The giveaway is the house number sitting
    immediately before the match, so that is what the test looks for.
    """
    words = name.split()
    while len(words) > 1 and words[0].upper().strip(",.") in STREETS:
        before = (text or "")[max(0, start - 14):start]
        if not re.search(r"\d\s*$", before):
            break
        # If a street TYPE follows, the street name is part of the company:
        # "Courtland Street LLC" is an entity, not the address "35 Courtland"
        # colliding with something else. Stripping it leaves "Street LLC".
        if _STREET_TYPE.match(words[1].strip(",.")):
            # "Courtland Street LLC" is an entity: nothing follows the street
            # type but a legal form. "846 Oaklawn Avenue DOMAIN REALTY, LLC"
            # is an address glued to one: real content follows, so drop both
            # the street name and its type.
            if len(words) > 3:
                words = words[2:]
                start += len(name) - len(" ".join(words))
                name = " ".join(words)
                continue
            break
        words = words[1:]
        start += len(name) - len(" ".join(words))
        name = " ".join(words)
    return name


def candidates_from(text):
    """Every company-shaped name in this text, with how it was found."""
    out = {}
    for tier, rx in (("labeled", LABELED), ("narrative", NARRATIVE),
                     ("legal_suffix", LEGAL), ("developer_word", DEVWORD),
                     ("shouted", SHOUT)):
        for m in rx.finditer(text or ""):
            n = _clean(_strip_address_prefix(m.group("n"), text, m.start("n")))
            if not n:
                continue
            if tier == "shouted":
                n = n.title()
            key = n.lower()
            if key in out:
                continue
            # A role title immediately after the name means this is staff or
            # counsel, not an applicant: "TOM KRAVITZ, DIRECTOR" is Warwick's
            # planning director on the department letterhead, and was being
            # recorded as the developer of the projects he was reviewing.
            after = (text or "")[m.end("n"):m.end("n") + 40]
            if ROLE_SUFFIX.match(after):
                continue
            i = max(0, m.start() - 90)
            out[key] = {
                "name": n,
                "found_as": tier,
                "quote": re.sub(r"\s+", " ", (text or "")[i:m.end() + 90]).strip(),
            }
    return list(out.values())


def _dedupe_substrings(cands):
    """Keep the fuller form. "E River LLC" is a truncation of "27 E River LLC"."""
    keep = []
    for c in sorted(cands, key=lambda x: -len(x["name"])):
        low = c["name"].lower()
        if any(low in k["name"].lower() for k in keep):
            continue
        keep.append(c)
    return keep


def rank(cands):
    """Operating companies before shells; labeled/narrative before incidental."""
    cands = _dedupe_substrings(cands)
    tier_rank = {"labeled": 0, "narrative": 0, "shouted": 1,
                 "developer_word": 1, "legal_suffix": 2}
    for c in cands:
        shell, rule = is_shell_name(c["name"])
        c["is_shell"] = bool(shell)
        c["shell_rule"] = rule
        c["has_legal_suffix"] = bool(has_legal_suffix(c["name"]))
        # TIER BEFORE SHELL. Ranking shells last put procedural boilerplate
        # above the real applicant whenever that applicant's name happened to
        # contain "Holding" or a street number -- "Major Change" beat "Walter
        # Bronhard and Brook Holding LLC", "Unified Development Review" beat
        # "121 Stamford Ave LLC". A named shell is a far better answer than a
        # heading. Then prefer a name that ENDS in a legal form, so OCR debris
        # ("Batwitwash UC RESUlt") loses to the clean form ("Batwitwash LLC").
        ends_legal = bool(LEGALWORD.match(c["name"].split()[-1].strip(",.;:")))
        c["_sort"] = (tier_rank.get(c["found_as"], 3), not ends_legal,
                      int(bool(shell)), -len(c["name"]))
    cands.sort(key=lambda c: c["_sort"])
    for c in cands:
        c.pop("_sort", None)
    return cands


def main():
    raw = load_items()
    groups = collapse(raw)

    text_by_key = {}
    for g in groups:
        muni = g["municipality"].lower()
        for it in g["items"]:
            k = (muni, normalize_address(it.get("address") or ""))
            text_by_key.setdefault(k, []).append(it)

    session = get_session()
    RI = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport")
    rows = (session.query(Project)
            .filter(Project.city.in_(RI))
            .filter((Project.developer.is_(None)) | (Project.developer == ""))
            .all())

    out = []
    for p in rows:
        key = (p.city.lower(), normalize_address(p.address or ""))
        items = text_by_key.get(key, [])
        blobs = [p.description or ""]
        if p.applicant_entity:
            blobs.append("Applicant: " + p.applicant_entity)
        for it in items:
            if it.get("description"):
                blobs.append(it["description"])
            if it.get("applicant_entity"):
                blobs.append("Applicant: " + it["applicant_entity"])
        text = "\n".join(dict.fromkeys(b for b in blobs if b))
        out.append({
            "id": p.id,
            "city": p.city,
            "address": p.address or "",
            "units": p.residential_units,
            "gsf": p.total_gsf,
            "asset_class": p.asset_class,
            "stage": p.stage_heard,
            "applicant_entity": p.applicant_entity,
            "source_items": len(items),
            "text_chars": len(text),
            "candidates": rank(candidates_from(text)),
        })

    # Largest first: the order the work gets done in.
    out.sort(key=lambda r: ((r["units"] or 0) * 400 + (r["gsf"] or 0)), reverse=True)
    OUT.write_text(json.dumps(out, indent=1), encoding="utf-8")

    n = len(out)
    withc = [r for r in out if r["candidates"]]
    oper = [r for r in out if any(not c["is_shell"] for c in r["candidates"])]
    log.info("Projects with no developer      : %d", n)
    log.info("  text found on disk            : %d", sum(1 for r in out if r["text_chars"] > 0))
    log.info("  extra items beyond the row    : %d", sum(1 for r in out if r["source_items"] > 0))
    log.info("  at least one candidate        : %d  (%.0f%%)", len(withc), 100 * len(withc) / n)
    log.info("  a NON-SHELL candidate         : %d  (%.0f%%)", len(oper), 100 * len(oper) / n)
    log.info("  no candidate at all           : %d", n - len(withc))
    log.info("Wrote %s", OUT)


if __name__ == "__main__":
    main()
