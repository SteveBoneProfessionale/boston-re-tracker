"""Choose which pages of an Article 80 filing to send to the model.

The corpus is not a set of 45-page documents. Median is 47 pages, mean 142,
max 1605, and 56% run past 40. A fixed 40-page head loses the "Architect:"
role label in 33% of the documents that carry one and the general-contractor
mention in 46% of the documents that carry one.

The signal is not uniformly distributed either: the team HEADER sits at
median page 3, but role labels sit at median page 30 and GC mentions at
median 35, scattered through appendices. So a head-slice is the wrong shape
entirely. Select by relevance instead -- the front matter, which establishes
the project and address, plus every page that actually names a role -- and
the result costs fewer tokens than the 40-page head while covering the tail.
"""
import re

HEAD_PAGES = 4          # cover + intro: establishes project name and address
MAX_PAGES = 24          # cost ceiling per document
CONTEXT = 1             # pages either side of a team header


def select(scan, max_pages=MAX_PAGES):
    """scan: one entry from data/dev_team_pages.json. Returns sorted page list (1-indexed)."""
    n = scan.get("pages", 0)
    if not n:
        return []
    head = set(range(1, min(HEAD_PAGES, n) + 1))

    # Priority order: a labelled role beats a header beats a bare mention.
    role = list(scan.get("role_pages") or [])
    header = []
    for p in scan.get("header_pages") or []:
        for d in range(-CONTEXT, CONTEXT + 1):
            if 1 <= p + d <= n:
                header.append(p + d)
    civil = list(scan.get("civil_pages") or [])
    gc = list(scan.get("gc_pages") or [])
    arch = list(scan.get("arch_pages") or [])

    # Round-robin across the buckets rather than draining them in order. A
    # document with forty "Architect:" pages would otherwise exhaust the
    # budget before the one page that names the general contractor.
    picked = set(head)
    buckets = [role, gc, civil, header, arch]
    seen = [set() for _ in buckets]
    i = 0
    while len(picked) < max_pages and any(
            any(p not in picked for p in b) for b in buckets):
        b, s_ = buckets[i % len(buckets)], seen[i % len(buckets)]
        for p in b:
            if p not in picked and p not in s_:
                picked.add(p)
                s_.add(p)
                break
        i += 1
        if i > 4000:
            break
    return sorted(picked)
