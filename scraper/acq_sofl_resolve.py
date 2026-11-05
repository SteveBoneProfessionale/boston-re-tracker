r"""Decide which operating firm stands behind a deed entity.

    python -m scraper.acq_sofl_resolve --report
    python -m scraper.acq_sofl_resolve --write

THE EVIDENCE RULE, taken from the brief and enforced here rather than applied
by eye:

  * A PRESS ARTICLE ABOUT THIS DEAL IS ENOUGH ON ITS OWN. It is reporting the
    transaction, not the paperwork, so one named firm settles it.

  * ANYTHING ELSE NEEDS TWO INDEPENDENT SIGNALS THAT AGREE. A registered
    manager alone is not enough. A c/o alone is certainly not enough -- a c/o
    is routinely a property manager, a law firm or a tax consultant, which is
    exactly why the brief calls it a clue rather than an answer.

  * OTHERWISE THE LLC STANDS. Nothing is inferred from a name that merely
    sounds like a firm. "Atlantic Gateway LLC" is not Atlantic Gateway
    Properties on the strength of two shared words.

WHAT COUNTS AS A SIGNAL, and why each is independent of the others:

    deed_co        the "C/O" or "%" party written on the deed itself, which
                   comes from the county's record of who to send mail to
    sunbiz_co      a "c/o FIRM" inside the entity's own registered principal
                   or mailing address, written by the filer
    sunbiz_mgr     a manager or authorized person that is itself a COMPANY
    sunbiz_parent  the managers of that company, one level up -- the chain the
                   brief asks to be followed until an operating company appears

Those are four different people writing in four different places, so two of
them agreeing is genuine corroboration. Two readings of the same field are
not, and _signals() never emits the same source twice for one firm.

SHELLS ARE FOLLOWED, NOT ACCEPTED. A manager named "LDHC1, LLC" sitting above
"LDHC HOLDINGS LLC" is the same family of shells, not the firm behind them, and
_is_shell() detects that by token overlap with the subject and by the
single-purpose shapes these names take -- a street number, a "PROPCO", an
"OWNER", a roman numeral. The chain is walked up to CHAIN_DEPTH levels, and if
every level still looks like a shell the entity stays unresolved. That is the
honest outcome and it is common.
"""

import argparse
import collections
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from scraper import acq_sofl_press as PRESS      # noqa: E402
from scraper import acq_sofl_sunbiz as SB        # noqa: E402

CHAIN_DEPTH = 3

# WHO WROTE THE SIGNAL, which is what independence actually turns on. The
# county wrote the deed's c/o; the entity's own filer wrote both its registered
# address and its manager list, so those two are one voice however many fields
# they fill; a parent company's own registration is a third voice again.
# Corroboration requires two different voices.
ORIGIN = {
    "deed_co": "deed",
    "sunbiz_co": "subject_filing",
    "sunbiz_mgr": "subject_filing",
    "sunbiz_parent": "parent_filing",
}

# Words that carry no identity. Two names sharing only these are not agreeing
# about anything -- "MIAMI PROPERTIES LLC" and "MIAMI HOLDINGS LLC" are
# different companies.
STOP = {
    "LLC", "L", "C", "LC", "INC", "LP", "LLP", "LTD", "CORP", "CORPORATION",
    "CO", "COMPANY", "PA", "PLLC", "TRUST", "TRS", "THE", "OF", "AND", "AT",
    "HOLDINGS", "HOLDING", "PROPERTIES", "PROPERTY", "REALTY", "REAL",
    "ESTATE", "INVESTMENTS", "INVESTMENT", "INVESTORS", "PARTNERS",
    "PARTNERSHIP", "GROUP", "VENTURES", "CAPITAL", "MANAGEMENT", "MGMT",
    "DEVELOPMENT", "DEV", "ASSOCIATES", "ENTERPRISES", "OWNER", "PROPCO",
    "SPE", "FL", "FLORIDA", "USA", "US", "LLC1", "I", "II", "III", "IV", "V",
    "VI", "VII", "VIII", "IX", "X",
}

# Name shapes that say "formed for one deal": a street number, a role word, a
# numeral suffix. Used only to decide whether to KEEP CLIMBING the chain.
SHELL_HINT = re.compile(
    r"^\d|\bPROPCO\b|\bOWNER\b|\bSPE\b|\bHOLDCO\b|\bMEZZ\b|\bTENANT\b|"
    r"\bSUBSIDIARY\b|\bPARCEL\b|\bBLOCK\b|\bLLC\d\b", re.I)


def _tokens(name):
    """Identifying words in a company name, suffixes and filler removed."""
    words = re.findall(r"[A-Z0-9&']+", str(name or "").upper().replace("-", " "))
    return [w for w in words if w not in STOP and len(w) > 1]


def _key(name):
    return frozenset(_tokens(name))


# A single shared word is only ever enough when the word itself is rare enough
# that two unrelated companies are unlikely to share it. "Kasumigaseki" is;
# "Goldman", "Miami" and "Atlantic" are not -- and the brief's instruction not
# to guess from a similar-sounding name is precisely about those.
_DISTINCTIVE = 8


def agree(a, b):
    """Whether two names identify the same firm.

    Deliberately strict. One name's identifying words must contain the
    other's entirely: "EL AD NATIONAL PROPERTIES LLC" and "El-Ad National
    Properties" agree because both reduce to {EL, AD, NATIONAL}.

    TWO IDENTIFYING WORDS ARE REQUIRED, equality included. Leaving equality
    exempt was a real hole: "GOLDMAN PROPERTY MGMT LLC" and "Goldman
    Properties" both reduce to {GOLDMAN} once the boilerplate is stripped, as
    do "MIAMI PROPERTIES LLC" and "MIAMI HOLDINGS LLC" to {MIAMI} -- and both
    pairs were being accepted as the same firm on one common word. The single
    exception is a word long enough to be a company's own coinage.
    """
    ka, kb = _key(a), _key(b)
    if not ka or not kb:
        return False
    small, large = (ka, kb) if len(ka) <= len(kb) else (kb, ka)
    if not small <= large:
        return False
    if len(small) >= 2:
        return True
    only = next(iter(small))
    return len(only) >= _DISTINCTIVE


def _is_shell(name, subject):
    """True when a name looks like another vehicle rather than the firm."""
    if SHELL_HINT.search(str(name or "")):
        return True
    ks, ku = _key(name), _key(subject)
    if ks and ku and (ks <= ku or ku <= ks):
        return True
    # A name with no identifying words left is pure boilerplate.
    return not ks


def base_names(entity):
    """The registered name(s) hiding inside a county's grantee string.

    THE COUNTY DOES NOT WRITE A COMPANY NAME, IT WRITES A MAILING LINE. A deed
    appears as "545 WYN OWNER LLC & C/O GOLDMAN PROPERTY MGMT LLC", and no such
    company is registered -- the entity is "545 WYN OWNER, LLC" and the rest is
    where to send the tax bill. Searching the register for the whole string
    finds nothing, which is exactly what happened on the first pass: all twenty
    entities carrying a c/o came back "not on the register" while several of
    them, Brookfield's among them, are plainly there under their own names.

    A string can also carry TWO grantees -- "RJR ML LLC & RJR REALTY LLC" --
    so both halves are returned and the caller may try each.
    """
    text = str(entity or "").strip()
    text = re.sub(r"\s*&\s*(?:C/O|%)\s*.+$", "", text, flags=re.I)
    parts = [p.strip() for p in re.split(r"\s+&\s+", text) if p.strip()]
    out, seen = [], set()
    for p in parts:
        if p.upper() not in seen and len(_tokens(p)) >= 1:
            seen.add(p.upper())
            out.append(p)
    return out or [text]


def deed_co(entity):
    """The c/o or % party the county records on the deed, if any.

    Miami-Dade writes both "X LLC & C/O Y" and "X LP & % Y"; Palm Beach writes
    neither. Returns only the trailing party, never the grantee itself.
    """
    text = str(entity or "")
    m = re.search(r"&\s*(?:C/O|%)\s*(.+)$", text, re.I)
    return m.group(1).strip() if m else None


def _sunbiz_co(record):
    """A "c/o FIRM" written inside a registered address."""
    out = []
    for field in ("principal_address", "mailing_address"):
        for line in str(record.get(field) or "").split("\n"):
            m = re.search(r"\bc/?o[:\s]\s*(.+)$", line.strip(), re.I)
            if m:
                name = m.group(1).strip(" ,.")
                # "c/o Bernice Lee, solely in her capacity as the court
                # appointed Receiver" is a person in a legal role, not a firm.
                if name and not re.search(r"\breceiver\b|\bcapacity\b", name, re.I):
                    out.append(name)
    return out


def _corporate_managers(record):
    """Managers that are companies rather than natural persons."""
    out = []
    for p in (record.get("persons") or []):
        name = (p.get("name") or "").strip()
        if re.search(r"\b(LLC|L\.L\.C|INC|LP|L\.P|LLP|LTD|CORP|COMPANY|"
                     r"PARTNERS|GROUP|TRUST|HOLDINGS|CAPITAL)\b", name, re.I):
            out.append(name)
    return out


def signals(entity, sunbiz, depth=CHAIN_DEPTH):
    """Every independent clue about the firm behind one deed entity.

    Returns [(source, name)]. Sources are distinct by construction, which is
    what makes "two signals that agree" mean two DIFFERENT kinds of evidence
    rather than one fact counted twice.
    """
    out = []

    co = deed_co(entity)
    if co and not _is_shell(co, entity):
        out.append(("deed_co", co))

    # The register knows the entity by its own name, not by the county's
    # mailing line -- see base_names. Try the harvested record first, then each
    # base name, falling back to a cached register lookup so that an entity
    # already on disk resolves even while the register is refusing new traffic.
    rec = sunbiz.get(entity)
    if not rec or rec.get("_error"):
        for base in base_names(entity):
            rec = sunbiz.get(base)
            if rec and not rec.get("_error"):
                break
            try:
                rec = SB.lookup(base)
            except Exception:                        # noqa: BLE001
                rec = None
            if rec:
                break
    if not rec or rec.get("_error"):
        return out

    for name in _sunbiz_co(rec):
        if not _is_shell(name, entity):
            out.append(("sunbiz_co", name))

    seen = {entity.upper()}
    frontier = [(entity, rec, 0)]
    while frontier:
        subject, record, level = frontier.pop(0)
        if level >= depth:
            continue
        for name in _corporate_managers(record):
            if name.upper() in seen:
                continue
            seen.add(name.upper())
            if not _is_shell(name, subject):
                out.append(("sunbiz_mgr" if level == 0 else "sunbiz_parent", name))
                continue
            # A shell: climb through it rather than reporting it.
            try:
                parent = SB.lookup(name)
            except Exception:                        # noqa: BLE001
                parent = None
            if parent:
                frontier.append((name, parent, level + 1))
    return out


def resolve(entity, sunbiz):
    """(firm, confidence, basis, url) for one deed entity, or None."""
    press = PRESS.lookup(entity)
    if press:
        return (press["firm"], "press_reported",
                f"{press['outlet']} on this deal: “{press['quote']}”",
                press["url"])

    sigs = signals(entity, sunbiz)
    if len(sigs) < 2:
        return None

    # TWO SIGNALS MUST COME FROM TWO DIFFERENT ORIGINS, not merely two
    # different fields. The register's "c/o" line and its manager list are both
    # written by the same filer on the same filing, so their agreeing is one
    # person saying something twice. The only resolution the loose rule ever
    # produced was exactly that shape -- a Doral warehouse whose registered
    # address and registered manager both read "GC Doral Holdings, LLC", which
    # corroborates nothing and is a holding shell besides.
    for i, (src_a, name_a) in enumerate(sigs):
        for src_b, name_b in sigs[i + 1:]:
            if ORIGIN[src_a] == ORIGIN[src_b] or not agree(name_a, name_b):
                continue
            # The longer name is usually the fuller one.
            firm = name_a if len(_key(name_a)) >= len(_key(name_b)) else name_b
            rec = sunbiz.get(entity) or {}
            return (tidy(firm), "two_signals",
                    f"{src_a} says {name_a!r} and {src_b} says {name_b!r}",
                    rec.get("sunbiz_url"))
    return None


def tidy(name):
    """A firm name fit to print, with the corporate suffix dropped."""
    text = re.sub(r"\s*,?\s*\b(LLC|L\.L\.C\.?|INC\.?|LP|L\.P\.?|LLP|LTD\.?|"
                  r"CORP\.?|CO\.?|PLLC|PA)\b\.?\s*$", "", str(name).strip(),
                  flags=re.I)
    if text.isupper() or text.islower():
        small = {"of", "and", "the", "for", "at", "in", "on", "de", "la"}
        parts = []
        for i, w in enumerate(text.split()):
            lw = w.lower()
            parts.append(lw if (i and lw in small) else
                         (w if len(w) <= 3 and w.isupper() and i == 0 else lw.capitalize()))
        text = " ".join(parts)
    return text.strip(" ,.&")


def load_sunbiz(path=None):
    path = path or os.path.join(ROOT, "data", "cache", "sunbiz_entities.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main():
    import sqlite3
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--sunbiz", default=None)
    args = ap.parse_args()

    sunbiz = load_sunbiz(args.sunbiz)
    db = os.path.join(ROOT, "data", "boston_re.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(
        "select id, buyer, seller, price from transactions "
        "where source in ('miamidade_pa','pbcpao')")]

    out = {}
    for r in rows:
        for side in ("buyer", "seller"):
            name = (r[side] or "").strip()
            if name and name not in out:
                out[name] = resolve(name, sunbiz)

    hit = {k: v for k, v in out.items() if v}
    print(f"{len(out)} entities, {len(hit)} resolved, {len(out) - len(hit)} "
          f"stay as the entity of record")
    by_conf = collections.Counter(v[1] for v in hit.values())
    for conf, n in by_conf.most_common():
        print(f"   {n:>4}  {conf}")

    if args.write:
        from scraper import acq_sofl_resolve_load
        acq_sofl_resolve_load.write(out)
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
