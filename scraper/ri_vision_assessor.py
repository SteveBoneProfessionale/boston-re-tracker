r"""
Year Built from the Vision assessor, for the Rhode Island cities Providence's
tax roll does not cover.

WHY THIS EXISTS. The completion sweep reached 52% of Rhode Island because the
assessor signal came from Providence's Socrata tax roll, and Warwick, Cranston
and Pawtucket are not on it. Those three are on Vision at gis.vgsi.com, which
publishes Year Built per parcel -- but only behind an ASP.NET form, so there is
no GET endpoint and a plain fetch returns the empty search page.

Vision's search form accepts MAP and LOT directly (txtM / txtL), which is the
key Rhode Island agendas actually give. So the match is on plat and lot rather
than on an address string, which is the more reliable join and the one that
worked for Providence.

THE SIGNAL, and its limit. Year Built at or after the project's first hearing
means a building went up after the filing, so the project is built. Year Built
BEFORE the first hearing means the parcel carries an older building -- that is
the same trap as a certificate of occupancy predating a filing, and it is
rejected, not counted. A parcel with no Year Built at all is unresolved.

Newport is not on Vision (404) and is not covered here.

    python scraper/ri_vision_assessor.py --dry-run
    python scraper/ri_vision_assessor.py --apply
"""

import re
import sys
import json
import time
import logging
import urllib.parse
import urllib.request
import http.cookiejar
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project, ProjectStageEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

OUT = ROOT / "data" / "ri_vision_assessor.json"
SITES = {"Cranston": "cranstonri", "Warwick": "warwickri", "Pawtucket": "pawtucketri"}
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"


def opener():
    cj = http.cookiejar.CookieJar()
    o = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    o.addheaders = [("User-Agent", UA)]
    return o


def hidden(html):
    out = {}
    for m in re.finditer(r'<input[^>]*type="hidden"[^>]*>', html, re.I):
        tag = m.group(0)
        n = re.search(r'name="([^"]+)"', tag)
        v = re.search(r'value="([^"]*)"', tag)
        if n:
            out[n.group(1)] = v.group(1) if v else ""
    return out


def parcel_fields(html):
    """Year Built, Living Area and Gross Area from a Vision parcel page."""
    txt = re.sub(r"<[^>]+>", " ", html)
    txt = re.sub(r"\s+", " ", txt)
    out = {}
    for label, key in (("Year Built", "year_built"), ("Living Area", "living_area"),
                       ("Gross Area", "gross_area"), ("Building Value", "building_value")):
        m = re.search(re.escape(label) + r"\s*:?\s*([\d,]+)", txt)
        if m:
            try:
                out[key] = int(m.group(1).replace(",", ""))
            except ValueError:
                pass
    m = re.search(r"Location\s*:?\s*([0-9][^:]{3,50}?)\s+(?:Owner|Mblu|Account)", txt)
    if m:
        out["location"] = m.group(1).strip()
    return out


def lookup(op, site, plat, lot, pause=0.7):
    base = "https://gis.vgsi.com/%s/" % site
    try:
        html = op.open(base + "Search.aspx", timeout=45).read().decode("utf-8", "replace")
    except Exception as e:                                      # noqa: BLE001
        return {"error": "search page: %s" % type(e).__name__}
    form = hidden(html)
    form.update({
        "ctl00$MainContent$txtSearchAddress": "",
        "ctl00$MainContent$txtSearchOwner": "",
        "ctl00$MainContent$txtSearchAcctNum": "",
        "ctl00$MainContent$txtM": str(plat),
        "ctl00$MainContent$txtL": str(lot),
        "ctl00$MainContent$txtU": "",
        "ctl00$MainContent$btnSubmit": "Search",
    })
    data = urllib.parse.urlencode(form).encode()
    try:
        r = op.open(urllib.request.Request(base + "Search.aspx", data=data,
                                           headers={"Content-Type": "application/x-www-form-urlencoded"}),
                    timeout=60)
        body = r.read().decode("utf-8", "replace")
        url = r.geturl()
    except Exception as e:                                      # noqa: BLE001
        return {"error": "post: %s" % type(e).__name__}
    time.sleep(pause)

    if "Parcel.aspx" in url:
        return parcel_fields(body)
    m = re.search(r"Parcel\.aspx\?pid=(\d+)", body)
    if not m:
        return {"error": "no parcel found"}
    try:
        p = op.open(base + "Parcel.aspx?pid=" + m.group(1), timeout=45).read().decode("utf-8", "replace")
    except Exception as e:                                      # noqa: BLE001
        return {"error": "parcel page: %s" % type(e).__name__}
    time.sleep(pause)
    return parcel_fields(p)


def platlots(p):
    raw = p.plat_lots_raw or ""
    m = re.search(r"(?:ap|a\.p\.|plat|assessor'?s?\s+plat)\s*\.?\s*(\d+)", raw, re.I)
    if not m:
        return []
    plat = m.group(1)
    return [(plat, l) for l in re.findall(r"\b(\d{1,5})\b", raw[m.end():])][:3]


def main(apply=False, limit=0):
    session = get_session()
    first = defaultdict(lambda: None)
    for e in session.query(ProjectStageEvent).all():
        d = str(e.meeting_date or "")[:10]
        if d and (first[e.project_id] is None or d < first[e.project_id]):
            first[e.project_id] = d

    rows = [p for p in session.query(Project).filter(Project.city.in_(list(SITES))).all()
            if not p.excluded and not p.completion_stage
            and p.project_status_filing not in ("Withdrawn", "Denied")]
    if limit:
        rows = rows[:limit]

    op = opener()
    built, older, none_found, nokey = [], [], [], []
    for i, p in enumerate(rows, 1):
        kk = platlots(p)
        if not kk:
            nokey.append(p.id)
            continue
        site = SITES[p.city]
        got = None
        for plat, lot in kk:
            r = lookup(op, site, plat, lot)
            if r.get("year_built"):
                got = (plat, lot, r)
                break
        if not got:
            none_found.append(p.id)
            continue
        plat, lot, r = got
        yb = r["year_built"]
        fd = first.get(p.id)
        rec = {"id": p.id, "city": p.city, "address": p.address, "plat": plat, "lot": lot,
               "year_built": yb, "living_area": r.get("living_area"),
               "gross_area": r.get("gross_area"), "first_hearing": fd}
        # Year Built must be at or after the filing, else it is an older
        # building on the same parcel -- the same guard the permit data needs.
        if fd and yb >= int(fd[:4]):
            built.append(rec)
        else:
            older.append(rec)
        if i % 10 == 0:
            log.info("  %d/%d  built=%d older=%d none=%d", i, len(rows), len(built), len(older), len(none_found))

    log.info("\nVISION ASSESSOR -- Cranston, Warwick, Pawtucket")
    log.info("  projects examined            : %d", len(rows))
    log.info("  no plat/lot to search on     : %d", len(nokey))
    log.info("  parcel found, no Year Built  : %d", len(none_found))
    log.info("  Year Built BEFORE filing     : %d  (older building, rejected)", len(older))
    log.info("  Year Built AT/AFTER filing   : %d  -> BUILT", len(built))
    for b in built:
        log.info("    id=%-4d %-11s %-34s plat %s lot %s  built %s  (first heard %s)",
                 b["id"], b["city"], str(b["address"])[:34], b["plat"], b["lot"],
                 b["year_built"], b["first_hearing"])

    OUT.write_text(json.dumps({"built": built, "older": older, "none": none_found,
                               "no_key": nokey}, indent=1), encoding="utf-8")

    if apply:
        for b in built:
            p = session.get(Project, b["id"])
            p.completion_stage = "Complete"
            p.completion_basis = "assessor_confirmed"
            p.completion_date = str(b["year_built"])
            p.completion_source_url = "https://gis.vgsi.com/%s/" % SITES[b["city"]]
            p.completion_evidence = (
                "Vision assessor, %s plat %s lot %s: Year Built %s%s. The project was first heard "
                "%s, so the building post-dates the filing rather than being an older structure on "
                "the same parcel." % (b["city"], b["plat"], b["lot"], b["year_built"],
                                      (", living area %s sq ft" % f"{b['living_area']:,}") if b.get("living_area") else "",
                                      b["first_hearing"]))
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "COMPLETE (assessor_confirmed): " + p.completion_evidence)
        session.commit()
        log.info("\nAPPLIED %d", len(built))
    else:
        log.info("\nDRY RUN -- re-run with --apply")
    session.close()


if __name__ == "__main__":
    lim = 0
    for a in sys.argv:
        if a.startswith("--limit="):
            lim = int(a.split("=")[1])
    main(apply="--apply" in sys.argv, limit=lim)
