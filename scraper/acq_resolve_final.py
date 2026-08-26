r"""Close out sponsor resolution across the 39-deal window.

Once the window narrowed to 2025-2026 the character of the problem changed
completely. The historical spine was 1,300 opaque single-purpose LLCs. This
window is 37 press rows, and press names firms: most "unresolved" entities here
were never throwaway vehicles at all, they were already Kensington Investment
Management, Phoenix Property Co., Morgan Stanley, LaSalle. They lacked a
canonical name only because no pattern in the table happened to match them.

So most of this file is canonicalisation, not research: taking the record entity
and writing the firm it plainly states, minus the qualifiers the press attached
("(entity)", "(affiliate)", "(Mai Luo affiliate)").

FOUR THINGS ARE NOT MERE TIDYING:

JSRE DECODED. 4-6 and 28 Newbury Street were held by FOUR-6 NEWBURY JSRE TIC LLC
and TWENTY 8 NEWBURY JSRE TIC LLC, two tenant-in-common vehicles whose shared
JSRE stem told us one group owned both but not who. Newmark's release and NEREJ
name it: the assets "traded from ASG EQUITIES to a joint venture between Acadia
Realty Trust and Osiris Ventures". ASG Equities is the New York family office of
the Gindi family, and this was at least its third Newbury Street disposal in a
year. $113.5M resolved on both sides.

THE BUYER ON THAT ROW WAS ALSO WRONG, in the way the joint-venture rule exists
to prevent: it read "Acadia Realty Trust" alone. Every source says Acadia AND
Osiris Ventures. Corrected to both.

AN INDIVIDUAL IS A VALID SPONSOR. Jeremy Seeger bought 23-25 Hammond Street and
Mohnsen Vessali sold 1848-1850 Commonwealth Avenue. There is no firm behind
them to find; the person IS the principal, and leaving these null would imply an
unfinished lookup rather than a completed one.

AN OPERATING COMPANY IS NOT AN SPE. Hackett Publishing Company sold 847
Massachusetts Avenue. It is a publisher that happened to own its building, and
it resolves to itself.

STILL UNRESOLVED, DELIBERATELY:
  HC 320 SUMMER ST      $26.3M. Bisnow notes Kendall Capital "picked up 320
                        Summer St. in Fort Point last year" in a June 2026
                        article, which points at 2025, not this February 2026
                        deed. The HC stem is undecoded and the timing does not
                        line up, so no sponsor is written.
  31 BUTTONWOOD STREET LLC / 14 WILLIS STREET LLC   $4.1M Dorchester. Pure
                        address-form vehicles on a deal too small to have been
                        written up. This is the long tail in miniature.

    python scraper/acq_resolve_final.py --apply
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# (row id, side, sponsor, confidence, basis, why)
FIXES = [
    # ── the one that needed research ────────────────────────────────
    (628, "seller", "ASG Equities", "web_corroborated", "web",
     'JSRE DECODED. Newmark\'s release and NEREJ ("Newmark arranges $113.5 '
     'million sale of 4-6 and 28 Newbury St. for ASG Equities") state the assets '
     '"traded from ASG Equities to a joint venture between Acadia Realty Trust '
     'and Osiris Ventures". ASG Equities is the New York family office of the '
     'Gindi family; Banker & Tradesman and Connect CRE corroborate. This decodes '
     'the JSRE stem shared by both tenant-in-common vehicles of record.'),
    (628, "buyer", "Acadia Realty Trust / Osiris Ventures", "web_corroborated",
     "web",
     'CORRECTED FROM "Acadia Realty Trust" ALONE. Newmark, NEREJ, Connect CRE, '
     'CPE and The Boston Globe all describe the buyer as a joint venture between '
     'Acadia Realty Trust and Osiris Ventures. Recording one partner would '
     'credit Acadia with $113.5M it did not commit by itself.'),

    # ── already-named firms, canonicalised ──────────────────────────
    (179, "buyer", "Synergy Investments / Axonic Capital", "web_corroborated", "web",
     "Joint venture; both partners recorded per the JV rule."),
    (179, "seller", "Clarendon Group", "web_corroborated", "web", None),
    (627, "seller", "Kensington Investment Management", "web_corroborated", "web", None),
    (887, "seller", "Phoenix Property Co.", "web_corroborated", "web", None),
    (888, "buyer", "Procter & Gamble", "web_corroborated", "web",
     "Recorded as P&G, the parent; Gillette is the operating brand named in the "
     "reporting."),
    (888, "seller", "Breakthrough Properties", "web_corroborated", "web", None),
    (901, "buyer", "J.T. Magen & Co. / Extell Development", "web_corroborated", "web",
     "Joint bidders retaining their own portfolio at auction; both recorded."),
    (181, "seller", "Capital Properties", "web_corroborated", "web", None),
    (182, "seller", "LaSalle Investment Management", "web_corroborated", "web", None),
    (890, "buyer", "Elliott Management", "web_corroborated", "web", None),
    (891, "buyer", "Treeco (The Real Estate Equity Co.)", "web_corroborated", "web", None),
    (891, "seller", "Kensington Investment Co.", "web_corroborated", "web", None),
    (892, "buyer", "Egeria Group", "web_corroborated", "web",
     'Record entity is an Egeria affiliate; the "(affiliate entity)" qualifier '
     "is a description of the vehicle, not part of the firm's name."),
    (183, "seller", "Morgan Stanley", "web_corroborated", "web", None),
    (178, "buyer", "Kendall Capital", "web_corroborated", "web",
     "Kendall Capital is the firm; Mai Luo is its principal, named in the "
     "reporting as the affiliate through which it held."),
    (893, "seller", "ASB Real Estate Investments", "web_corroborated", "web", None),
    (185, "buyer", "Giri Hospitality", "web_corroborated", "web", None),
    (185, "seller", "Hawkins Way Capital", "web_corroborated", "web", None),
    (184, "buyer", "Hudson Assembly / Time Equities", "web_corroborated", "web",
     "Hudson Assembly is itself a joint venture of Evan Papanastasiou and Noam "
     "Ron, backed by Time Equities as capital partner; both recorded."),
    (184, "seller", "Northwood Investors", "web_corroborated", "web", None),
    (896, "buyer", "True North Legacy Holdings", "web_corroborated", "web",
     "Jeffrey R. Bruce is the founding principal, named in the reporting."),
    (896, "seller", "MG2 Group", "web_corroborated", "web",
     "Bisnow names MG2 Group; The Real Reporter names Joseph Donovan of EB3 "
     "Holdings. Not reconciled; the deed-sourced name is recorded."),
    (897, "buyer", "Sunrise Capital Investors / Parking Advisors",
     "web_corroborated", "web", "Joint venture; both partners recorded."),
    (899, "buyer", "Embrace Boston", "web_corroborated", "web", None),
    (899, "seller", "Kendall Capital", "web_corroborated", "web", None),
    (898, "buyer", "ARX", "web_low_confidence", "web",
     "The Real Reporter names only ARX and is paywalled past the lead; no "
     "address, no seller, no fuller firm name available."),

    # ── individuals, who are the principal ──────────────────────────
    (632, "buyer", "Jeremy Seeger", "web_corroborated", "web",
     "An individual buyer, not a vehicle. Bisnow names him directly."),
    (632, "seller", "Cafasso Properties", "web_corroborated", "web", None),
    (894, "seller", "Mohnsen Vessali", "web_corroborated", "web",
     "An individual seller named in the deed reporting."),
    (903, "seller", "Sozio family", "web_low_confidence", "web",
     "The Real Reporter identifies the sellers as the family of Angelo 'Chuck' "
     "Sozio; no corporate vehicle is named and the article is paywalled past "
     "the lead."),

    # ── an operating company that owned its own building ────────────
    (633, "seller", "Hackett Publishing Company", "registry_confirmed", "registry",
     "Not a single-purpose vehicle: a publisher that owned its premises at 847 "
     "Massachusetts Avenue and sold them. Resolves to itself."),

    (631, "buyer", "Groma", "web_corroborated", "web", None),
]

# Rows left unresolved on purpose, with the reason recorded on the row so a
# later pass does not re-litigate them from scratch.
LEAVE = [
    (893, "buyer",
     "SPONSOR NOT ESTABLISHED. The record entity is HC 320 SUMMER ST., an "
     "address-form vehicle whose HC stem is undecoded. Bisnow's June 2026 "
     "reporting notes that Kendall Capital \"picked up 320 Summer St. in Fort "
     "Point last year\", which points at 2025 rather than this February 2026 "
     "deed, so the obvious candidate does not fit the date. No sponsor written."),
    (631, "seller",
     "SPONSOR NOT ESTABLISHED. 31 BUTTONWOOD STREET LLC / 14 WILLIS STREET LLC "
     "are pure address-form vehicles on a $4.1M Dorchester deal that no "
     "publication covered beyond naming the buyer. This is the long tail in "
     "miniature: the entity names carry no stem, the vehicles mail to their own "
     "assets so address clustering finds nothing, and the deal is far too small "
     "for trade press to name a principal. The Secretary of the Commonwealth "
     "filing would answer it and is behind the Imperva block."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, conf, basis, why in FIXES:
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s not found", rid)
            continue
        entity, existing = cur
        if existing == sponsor:
            continue
        log.info("id=%-4s %-6s %-44s -> %s", rid, side, (entity or "")[:44], sponsor)
        if not dry_run:
            note = (f" | {side.upper()} RESOLVED TO SPONSOR. Record entity kept "
                    f"verbatim in `{side}`.")
            if why:
                note += " " + why
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = :c,
                       {side}_resolution_basis = :b,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""),
                {"s": sponsor, "c": conf, "b": basis, "n": note, "id": rid})
        n += 1

    for rid, side, why in LEAVE:
        if not dry_run:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id and coalesce(notes,'') not like :chk"),
                {"n": " | " + why, "id": rid, "chk": "%SPONSOR NOT ESTABLISHED%"})
        log.info("id=%-4s %-6s left unresolved, reason recorded", rid, side)

    if not dry_run:
        conn.commit()

    tot, vol = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions")).first()
    log.info("\n%d rows updated. Table: %d transactions, $%.2fB", n, tot, vol / 1e9)
    for side in ("buyer", "seller"):
        have = conn.execute(text(
            f"select count(*) from transactions "
            f"where coalesce({side},'') <> ''")).scalar()
        res, rv = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce({side}_canonical,'') <> ''")).first()
        log.info("%-7s entity on %2d rows | sponsor on %2d of %d (%.0f%%), $%.2fB "
                 "(%.0f%% of dollars)", side, have, res, tot, res / tot * 100,
                 (rv or 0) / 1e9, (rv or 0) / vol * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
