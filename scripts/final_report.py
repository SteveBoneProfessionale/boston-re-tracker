"""Produce the end-of-run report: fill, tiers, failures, and what was not reached."""
import json
import random
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from provenance import construction_started

FIELDS = ("architect", "civil_engineer", "general_contractor")
RI = {"Providence", "Cranston", "Warwick", "Pawtucket", "Newport"}
OUT = []


def p(s=""):
    OUT.append(s)
    # The corpus carries typographic dashes and quotes that a cp1252 console
    # cannot encode; the file keeps them, the console gets a safe rendering.
    try:
        print(s)
    except UnicodeEncodeError:
        print(s.encode("ascii", "replace").decode("ascii"))


def market(city):
    if city == "Boston":
        return "Boston"
    if city == "Cambridge":
        return "Cambridge"
    if city in RI:
        return "Rhode Island"
    if not city:
        return "(null city)"
    return "Other MA"


def main():
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    base = sqlite3.connect("data/_baseline_prerun.db")
    base.row_factory = sqlite3.Row

    rows = c.execute("select id,name,address,city,status,completion_stage,"
                     "architect,civil_engineer,general_contractor "
                     "from projects where coalesce(excluded,0)=0").fetchall()
    byid = {r["id"]: r for r in rows}
    prior = {r["id"]: r for r in base.execute(
        "select id,architect,civil_engineer,general_contractor from projects "
        "where coalesce(excluded,0)=0")}
    live = {(r["project_id"], r["field"]): r for r in
            c.execute("select * from field_provenance where superseded=0")}

    def is_resolved(pid, f):
        v = live.get((pid, f))
        return v is not None and v["outcome"] == "resolved"

    # ---------- 1. fill rate per field per market ----------
    p("=" * 96)
    p("1. FILL RATE PER FIELD PER MARKET   (727 non-excluded records)")
    p("=" * 96)
    mk = defaultdict(list)
    for r in rows:
        mk[market(r["city"])].append(r)
    p(f"{'market':14} {'n':>4}   " + "   ".join(f"{f:>26}" for f in FIELDS))
    p(f"{'':14} {'':>4}   " + "   ".join(f"{'before -> after':>26}" for _ in FIELDS))
    tot_b, tot_a = Counter(), Counter()
    for m in ("Boston", "Cambridge", "Rhode Island", "Other MA", "(null city)"):
        rs = mk.get(m, [])
        if not rs:
            continue
        cells = []
        for f in FIELDS:
            b = sum(1 for r in rs if (prior[r["id"]][f] or "").strip())
            # Count the live table, not the provenance index: the 13 null-city
            # rows were left untouched by instruction and carry no provenance.
            a = sum(1 for r in rs
                    if (r[f] or "").strip() and r[f] != "not_yet_selected")
            tot_b[f] += b
            tot_a[f] += a
            cells.append(f"{b:3} ({100*b/len(rs):3.0f}%) -> {a:3} ({100*a/len(rs):3.0f}%)")
        p(f"{m:14} {len(rs):>4}   " + "   ".join(f"{x:>26}" for x in cells))
    n = len(rows)
    cells = [f"{tot_b[f]:3} ({100*tot_b[f]/n:3.0f}%) -> {tot_a[f]:3} ({100*tot_a[f]/n:3.0f}%)"
             for f in FIELDS]
    p(f"{'ALL':14} {n:>4}   " + "   ".join(f"{x:>26}" for x in cells))
    p("  (the 13 null-city rows were skipped by instruction; their counts are")
    p("   unchanged and they carry no provenance rows)")

    started = [r for r in rows if construction_started(r)]
    notstarted = [r for r in rows if not construction_started(r)]
    gc_started = sum(1 for r in started if is_resolved(r["id"], "general_contractor"))
    nys = sum(1 for r in notstarted
              if (live.get((r["id"], "general_contractor")) or {})
              and live[(r["id"], "general_contractor")]["outcome"] == "not_yet_selected")
    gnull = sum(1 for r in started
                if (live.get((r["id"], "general_contractor")) or {})
                and live[(r["id"], "general_contractor")]["outcome"] == "null")
    p()
    p("GENERAL CONTRACTOR reported against both denominators, as asked")
    p(f"  construction started:          {gc_started:3}/{len(started):3} resolved "
      f"({100*gc_started/max(1,len(started)):.0f}%)")
    p(f"  construction started, null:    {gnull:3}/{len(started):3} searched and empty")
    p(f"  not yet under construction:    {nys:3}/{len(notstarted):3} recorded not_yet_selected")

    # ---------- 2. tier distribution ----------
    p()
    p("=" * 96)
    p("2. TIER DISTRIBUTION PER FIELD   (resolved values only)")
    p("=" * 96)
    tiers = ["document_confirmed", "registry_confirmed", "web_corroborated",
             "web_low_confidence", "unverified_prior"]
    p(f"{'tier':24}" + "".join(f"{f:>22}" for f in FIELDS))
    for t in tiers:
        cnt = [sum(1 for k, v in live.items() if k[1] == f and v["outcome"] == "resolved"
                   and v["tier"] == t) for f in FIELDS]
        p(f"{t:24}" + "".join(f"{x:>22}" for x in cnt))
    p(f"{'TOTAL resolved':24}" + "".join(
        f"{sum(1 for k, v in live.items() if k[1] == f and v['outcome'] == 'resolved'):>22}"
        for f in FIELDS))
    p()
    p("  by market:")
    for m in ("Boston", "Cambridge", "Rhode Island", "Other MA", "(null city)"):
        ids = {r["id"] for r in mk.get(m, [])}
        cc = Counter(v["tier"] for k, v in live.items()
                     if k[0] in ids and v["outcome"] == "resolved" and v["tier"])
        if cc:
            p(f"    {m:14} " + ",  ".join(f"{k}={v}" for k, v in cc.most_common()))

    # ---------- 3. failed verification ----------
    p()
    p("=" * 96)
    p("3. PREVIOUSLY POPULATED VALUES THAT FAILED VERIFICATION")
    p("=" * 96)
    av = Counter((r["field"], r["verdict"]) for r in
                 c.execute("select field,verdict from prior_value_audit"))
    for f in ("architect", "civil_engineer"):
        conf, rnl, fa = av[(f, "confirmed")], av[(f, "role_not_labelled")], av[(f, "firm_absent")]
        tot = conf + rnl + fa
        p(f"  {f:16} checked {tot:3}   confirmed {conf:3}   role-not-labelled {rnl:3}   "
          f"firm-absent {fa:3}   FAILED {rnl + fa:3} ({100*(rnl+fa)/max(1,tot):.0f}%)")
    dropped = c.execute("select count(*) from field_provenance where superseded=0 "
                        "and reason like '%failed verification%'").fetchone()[0]
    repl = c.execute("select count(*) from prior_value_audit a "
                     "where a.verdict in ('role_not_labelled','firm_absent') "
                     "and exists (select 1 from field_provenance f "
                     "where f.project_id=a.project_id and f.field=a.field "
                     "and f.superseded=0 and f.outcome='resolved')").fetchone()[0]
    unv = sum(1 for v in live.values() if v["tier"] == "unverified_prior")
    p(f"  dropped outright (failed, nothing better found):        {dropped}")
    p(f"  failed but replaced by a better value from the same doc: {repl}")
    p(f"  kept as unverified_prior (no readable primary document): {unv}")
    p()
    p("  the values dropped:")
    for r in c.execute("select project_id, field, reason from field_provenance "
                       "where superseded=0 and reason like '%failed verification%' "
                       "order by field, project_id"):
        val = r["reason"].split('"')[1] if '"' in r["reason"] else "?"
        why = "role not labelled" if "role_not_labelled" in r["reason"] else "firm absent"
        pr = byid.get(r["project_id"])
        addr = str(pr["address"])[:26] if pr else ""
        p(f"    {r['project_id']:4} {addr:26} {r['field']:16} {val[:32]:32} {why}")

    # ---------- 4. step 4 coverage ----------
    p()
    p("=" * 96)
    p("4. STEP 4 COVERAGE -- WHAT WAS SEARCHED AND WHAT WAS NOT")
    p("=" * 96)
    searched = {r[0] for r in c.execute("select distinct project_id from field_evidence")}
    searched |= {r[0] for r in c.execute(
        "select distinct project_id from field_provenance where resolution_step=4")}
    queue = json.loads(Path("data/step4_queue.json").read_text())
    qids = {q["project_id"] for q in queue}
    notreached = [q for q in queue if q["project_id"] not in searched]
    p(f"  projects entering Step 4 needing at least one field: {len(qids)}")
    p(f"  projects searched:                                   {len(searched & qids)}")
    p(f"  projects NOT reached:                                {len(notreached)}")
    byp = Counter(q["priority"] for q in notreached)
    lbl = {1: "Boston/Cambridge architect or civil missing",
           2: "GC, construction already started",
           3: "Boston/Cambridge corroborate unverified",
           4: "other MA", 5: "Rhode Island"}
    for k in sorted(byp):
        p(f"      priority {k}  {lbl[k]:44} {byp[k]:4}")
    Path("data/step4_not_reached.json").write_text(json.dumps(notreached, indent=1))
    p("  complete list: data/step4_not_reached.json")
    p()
    p("  the priority-2 projects not reached (GC, construction started) -- the most")
    p("  consequential gap, listed in full:")
    for q in [x for x in notreached if x["priority"] == 2]:
        p(f"    {q['project_id']:4} {str(q['city'])[:11]:11} {str(q['address'])[:34]:34} "
          f"{str(q['status'])[:20]:20}")

    # ---------- 5. null city ----------
    p()
    p("=" * 96)
    p("5. THE 13 NULL-CITY ROWS (skipped as agreed, listed here)")
    p("=" * 96)
    for r in c.execute("select id,name,address,status,architect,civil_engineer,"
                       "general_contractor from projects where city is null "
                       "and coalesce(excluded,0)=0 order by id"):
        p(f"  {r['id']:4} {str(r['name'])[:44]:44} | {str(r['address'])[:24]:24} | "
          f"{str(r['status'])[:18]}")
        p(f"        architect={str(r['architect'])[:30]:30} "
          f"civil={str(r['civil_engineer'])[:26]:26} gc={r['general_contractor']}")

    # ---------- 6. spot check ----------
    p()
    p("=" * 96)
    p("6. SPOT-CHECK SAMPLE -- 40 records, weighted toward web_low_confidence")
    p("=" * 96)
    rnd = random.Random(20260819)
    weight = {"web_low_confidence": 6, "web_corroborated": 4, "registry_confirmed": 2,
              "document_confirmed": 1, "unverified_prior": 1}
    pool = []
    for k, v in live.items():
        if v["outcome"] != "resolved":
            continue
        pool += [k] * weight.get(v["tier"], 1)
    rnd.shuffle(pool)
    seen, sample = set(), []
    for k in pool:
        if k in seen:
            continue
        seen.add(k)
        sample.append(k)
        if len(sample) == 40:
            break
    for (pid, f) in sample:
        v = live[(pid, f)]
        pr = byid.get(pid)
        p(f"  {pid:4} {str(pr['city'])[:11]:11} {str(pr['address'])[:26]:26} "
          f"{f:18} {str(v['value'])[:28]:28} [{v['tier']}]")
        src = v["source_url"] or v["source_name"] or ""
        p(f"       source: {str(src)[:104]}")
        if v["page_ref"]:
            p(f"       page/ref: {v['page_ref']}")
        if v["firm_sentence"]:
            p(f"       \"{str(v['firm_sentence'])[:160]}\"")
        p()

    Path("data/final_report.txt").write_text("\n".join(OUT), encoding="utf-8")
    print("wrote data/final_report.txt")


if __name__ == "__main__":
    main()
