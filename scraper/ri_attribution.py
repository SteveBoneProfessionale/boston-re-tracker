r"""
Deciding which agenda item a fact belongs to.

A planning agenda is a list of unrelated projects printed end to end. Any
extractor reading a window of that text will sooner or later hand one
project's number to its neighbour, and the result is not a visible gap but a
plausible wrong figure -- the failure mode that matters most here.

Real examples this module exists to stop, every one of them a live bug:

  * Champlin Heights, 152 dwellings, was labelled Major on the strength of a
    "Legion Bowl" item printed directly beneath it.
  * 45-47 America St and 201 Arlington Ave both "held" as Major on the same
    verbatim agenda footer quoting a different case entirely.
  * A 210,000 sq ft warehouse at 20 Goddard Drive was offered as the floor
    area of BOTH Champlin Heights and the Independence Way Proposal.
  * 532's developer was mis-extracted as "Champlin Heights II, LLC" from the
    item above it -- and that wrong value then acted as an identity anchor,
    matching the very block it had been copied from.

Two checks, and they compose:

  is_shared()    the same evidence string appearing under several projects
                 is boilerplate and supports none of them. Identical
                 evidence under two projects is the tell.
  nearest()      within a block, the fact belonging to a project is the one
                 nearest that project's OWN identifier. Anything further
                 than max_gap away belongs to a different item.

anchors_for() deliberately reads identity fields only -- plat and lot, case
number, address, project name. Never developer or applicant: those are
themselves extracted, and anchoring on them is circular.
"""

import re
from collections import Counter

# Where one agenda item ends and the next begins.
#   Cranston  bullets a named project:  # "Champlin Heights" (vote taken)
#   Providence numbers it:              Case no. 25-075MA - 195 Nelson Street
#   Pawtucket  marks nothing at all, so its whole agenda is one block and
#              proximity does all the work.
ITEM_SPLIT = re.compile(
    r"(?=[▪●•■◦])"
    r"|(?=\bCase\s+(?:no|No|NO)\.?\s*\d)"
    r"|(?=\bReferral\s+(?:no|No|NO)\.?\s*\d)"
    r"|(?=\bAGENDA\s+ITEM\b)")

STREET = (r"street|st|avenue|ave|road|rd|drive|dr|boulevard|blvd|lane|ln|way|"
          r"place|pl|court|ct|highway|hwy|terrace|ter")


def item_blocks(text):
    return [b for b in ITEM_SPLIT.split(text) if b and b.strip()]


def anchors_for(p):
    """Identity tokens for one project. Identity fields ONLY -- see module doc."""
    out = set()
    for v in (getattr(p, "plat_lots_raw", None), getattr(p, "case_number", None)):
        if v and len(str(v).strip()) > 3:
            out.add(re.sub(r"\s+", " ", str(v).strip()).lower())
    addr = re.sub(r"\s+", " ", (getattr(p, "address", None) or "").strip()).lower()
    if len(addr) > 5:
        out.add(addr)
        # "282 East Avenue" is also printed "282 East Ave."
        m = re.match(r"(\d+[\w-]*)\s+(.+?)\s+(?:%s)\b" % STREET, addr)
        if m:
            out.add("%s %s" % (m.group(1), m.group(2)))
    name = re.sub(r"\s+", " ", (getattr(p, "name", None) or "").strip()).lower()
    if len(name) > 4:
        out.add(name)
    return out


def anchor_positions(block, anchors):
    low, pos = block.lower(), []
    for a in anchors:
        i = low.find(a)
        while i >= 0:
            pos.append(i)
            i = low.find(a, i + 1)
    return pos


def nearest(block, anchors, matches, max_gap=700):
    """The match nearest an occurrence of this project's identifier.

    `matches` is any iterable of objects with .start(), i.e. re match objects.
    Returns (match, distance) or (None, None) when nothing is close enough --
    which means the fact belongs to another item and must not be used.
    """
    pos = anchor_positions(block, anchors)
    if not pos:
        return None, None
    best, bestd = None, None
    for m in matches:
        d = min(abs(m.start() - a) for a in pos)
        if bestd is None or d < bestd:
            best, bestd = m, d
    if best is None or bestd > max_gap:
        return None, None
    return best, bestd


def block_containing(text, anchors, wide=None):
    """Blocks naming this project, most specific source first."""
    out = []
    for src in (wide, text):
        if not src:
            continue
        for b in item_blocks(src):
            if anchor_positions(b, anchors):
                out.append(b)
    return out


def shared_strings(evidence_by_project, key_len=90):
    """Evidence appearing under more than one project. Boilerplate."""
    c = Counter()
    for evs in evidence_by_project.values():
        for e in dict.fromkeys(x[:key_len] for x in evs):
            c[e] += 1
    return {k for k, n in c.items() if n > 1}


def is_shared(ev, shared, key_len=90):
    return ev[:key_len] in shared
