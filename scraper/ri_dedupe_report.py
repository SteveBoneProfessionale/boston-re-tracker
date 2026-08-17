r"""
Measure the deduplication collapse rate over the harvested agenda corpus.

Reports how many raw agenda items collapse into how many projects, per
municipality, plus a sample of collapses for manual audit. Weighted toward
Warwick, where matching is weakest: plat/lot appears on only 19% of Warwick
development items, so address carries the identity there.

Parcel and address extraction here is regex-based, not LLM-based. That is
enough to measure collapse behaviour, which depends only on parcel and address,
and it runs without API credits. Full field extraction is a separate step.

    python scraper/ri_dedupe_report.py --sample 20
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.ri_identity import collapse, parcel_id, same_project, ADDRESS_PRIMARY

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

CORPUS = Path(__file__).parent.parent / "data" / "ri_agenda_corpus.json"
TEXT_DIR = Path(__file__).parent.parent / "data" / "ri_pdfs" / "text"
OUT = Path(__file__).parent.parent / "data" / "ri_dedupe_report.json"

# A parcel reference plus the surrounding clause, which carries the address.
_PARCEL_CLAUSE = re.compile(
    r"((?:TAP|A\.?P\.?|(?:tax\s*)?assessors?'?\s*plat|plat)\s*\.?\s*#?\s*\d+"
    r"[^.;\n]{0,120})", re.I)
# Address forms: "at 80 Erastus Street", or a leading "46 East Bowery Street;"
_ADDR_AFTER = re.compile(r"\bat\s+(\d[\w\s\-]{2,44}?(?:ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|"
                         r"LN|LANE|BLVD|PL|PLACE|CT|COURT|TER|WAY|HWY|SQ))\b", re.I)
_ADDR_BEFORE = re.compile(r"(\d[\w\s\-]{2,44}?(?:ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|"
                          r"LN|LANE|BLVD|PL|PLACE|CT|COURT|TER|WAY|HWY|SQ))\b[;,]", re.I)

# Events that appear in history but never advance the current stage.
_NON_ADVANCING = re.compile(
    r"\b(extension|extend|modification|amend|continued|continuance|waiver)\b", re.I)


def extract_items() -> list[dict]:
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    items = []
    for meeting in corpus.values():
        for doc in meeting["documents"]:
            p = TEXT_DIR / doc["text_file"]
            if not p.exists():
                continue
            text = p.read_text(encoding="utf-8", errors="replace")
            if len(text) < 200:
                continue
            for m in _PARCEL_CLAUSE.finditer(text):
                clause = re.sub(r"\s+", " ", m.group(1)).strip()
                # Take the address from the parcel's own clause first. A wide
                # window bleeds the neighbouring agenda item's address in, which
                # then chains two unrelated parcels together during collapse.
                window = text[max(0, m.start() - 120): m.end() + 60]
                am = _ADDR_AFTER.search(clause) or _ADDR_BEFORE.search(clause) \
                    or _ADDR_AFTER.search(window)
                items.append({
                    "municipality": meeting["municipality"],
                    "board": meeting["board"],
                    "date": meeting["date"],
                    "plat_lot_text": clause,
                    "address": (am.group(1).strip() if am else ""),
                    "non_advancing": bool(_NON_ADVANCING.search(window)),
                    "source_url": doc["url"],
                })
    return items


def main(sample_n: int):
    items = extract_items()
    log.info("Raw agenda items with a parcel reference: %d\n", len(items))

    by_muni = defaultdict(list)
    for it in items:
        by_muni[it["municipality"]].append(it)

    report = {"per_municipality": {}, "sample": []}
    all_groups = []

    log.info("%-12s %>0s" % ("", "") if False else
             f"{'MUNICIPALITY':13} {'raw items':>10} {'projects':>10} {'collapse':>10} {'flagged':>9}")
    for muni in sorted(by_muni):
        groups = collapse(by_muni[muni])
        for g in groups:
            g["municipality"] = muni
        all_groups.extend(groups)
        raw, proj = len(by_muni[muni]), len(groups)
        flagged = sum(1 for g in groups if g["needs_review"])
        rate = (1 - proj / raw) * 100 if raw else 0
        report["per_municipality"][muni] = {
            "raw_items": raw, "projects": proj,
            "collapse_pct": round(rate, 1), "flagged_for_review": flagged,
            "address_primary": muni in ADDRESS_PRIMARY,
        }
        log.info(f"{muni:13} {raw:>10} {proj:>10} {rate:>9.1f}% {flagged:>9}")

    raw_t = len(items)
    proj_t = len(all_groups)
    log.info(f"{'TOTAL':13} {raw_t:>10} {proj_t:>10} "
             f"{(1 - proj_t / raw_t) * 100 if raw_t else 0:>9.1f}% "
             f"{sum(1 for g in all_groups if g['needs_review']):>9}")

    # Sample of real collapses, weighted toward Warwick where matching is
    # weakest and therefore most in need of an audit.
    multi = [g for g in all_groups if len(g["items"]) > 1]
    warwick = [g for g in multi if g["municipality"] in ADDRESS_PRIMARY]
    others = [g for g in multi if g["municipality"] not in ADDRESS_PRIMARY]
    n_w = min(len(warwick), max(sample_n // 2, 1))
    chosen = warwick[:n_w] + others[: sample_n - n_w]

    log.info("\n=== COLLAPSE SAMPLE (%d groups, %d from Warwick) ===\n",
             len(chosen), n_w)
    for i, g in enumerate(chosen, 1):
        flag = "  [REVIEW]" if g["needs_review"] else ""
        log.info("[%2d] %s — %d items -> 1 project%s", i, g["municipality"],
                 len(g["items"]), flag)
        for it in g["items"]:
            log.info("       %s  %-46s %s", it["date"],
                     it["plat_lot_text"][:46], it["address"][:30])
        for r in g["reasons"]:
            log.info("       matched: %s", r)
        report["sample"].append({
            "municipality": g["municipality"],
            "needs_review": g["needs_review"],
            "reasons": g["reasons"],
            "items": [{k: it[k] for k in ("date", "board", "plat_lot_text",
                                          "address", "source_url")}
                      for it in g["items"]],
        })

    OUT.write_text(json.dumps(report, indent=1), encoding="utf-8")
    log.info("\nWritten to %s", OUT)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=20)
    main(ap.parse_args().sample)
