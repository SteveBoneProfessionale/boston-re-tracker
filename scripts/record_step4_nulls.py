"""Record the fields that were searched in Step 4 and did not resolve.

A null with a reason is the correct output. Each entry below names what was
searched and why nothing was taken -- most often because a source named a
firm without labelling the role, or named it for a different building.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from provenance import connect, construction_started, record

A, C, G = "architect", "civil_engineer", "general_contractor"

UNRESOLVED = [
    (275, G, "sources found describe One Boston Wharf Road (Block L5); this record is "
             "Block L3 & L6, so the GC named there was not carried across"),
    (53, G, "searched project name and address; architect (KPF) named, no contractor"),
    (247, G, "searched; design team named (CBT, Howard Stein Hudson) but no contractor"),
    (353, G, "searched; architect (CBT) named, no contractor"),
    (243, G, "searched; no contractor named in any non-aggregator source"),
    (287, A, "searched; sources say the developer 'hired multiple architectural and "
             "engineering teams' without naming them"),
    (287, C, "searched; no civil engineer named"),
    (167, C, "searched; design team named as Tishman Speyer, Payette and Mikyong Kim "
             "Design -- none labelled civil engineer"),
    (240, A, "searched; sources cover the historic Hotel Buckminster, not the current filing"),
    (240, C, "searched; no civil engineer named"),
    (314, A, "searched; no design team named in any source"),
    (314, C, "searched; no civil engineer named"),
    (463, A, "searched; Elkus Manfredi appears only as an image credit, which does not "
             "label the role"),
    (463, C, "searched; no civil engineer named"),
    (422, A, "searched; Elkus Manfredi named generically for Kendall Square work, not "
             "for this record"),
    (422, C, "searched; no civil engineer named"),
    (276, A, "searched; no architect named for Block G in any non-aggregator source"),
    (385, C, "searched; no civil engineer named"),
    (385, G, "searched; Skanska is developer and builder but no source labels a GC"),
    (414, C, "searched; no civil engineer named"),
    (414, G, "searched; Turner appears only in a lawsuit reference, which does not "
             "label the role"),
    (409, C, "searched; no design team named"),
    (409, G, "searched; no contractor named"),
    (468, A, "searched; filing is massing-only, no design team named"),
    (468, C, "searched; no civil engineer named"),
    (430, C, "searched; sources are leasing and ownership records only"),
    (430, G, "searched; no contractor named"),
    (346, A, "searched; Stantec confirmed for Allston Yards Building A only, not Building C"),
    (346, C, "searched; no civil engineer named for Building C"),
    (345, A, "searched; Stantec confirmed for Building A only, not Building D"),
    (345, C, "searched; no civil engineer named for Building D"),
    (358, G, "searched; Lee Kennedy's completion notice refers to 100 Hood Park Drive, "
             "not 10 Stack Street"),
    (358, C, "searched; no civil engineer named"),
    (24, A, "searched; no design team named in any source"),
    (24, C, "searched; no civil engineer named"),
    (27, A, "searched; sources describe the 1910-1916 campus buildings, not this filing"),
    (27, C, "searched; no civil engineer named"),
    (279, A, "searched; sources describe an earlier 16-bed med/surg fit-out, not this "
             "new inpatient building"),
    (279, C, "searched; no civil engineer named"),
    (279, G, "searched; Consigli named for the earlier fit-out, not this filing"),
    (88, A, "searched; the meeting report places David Manfredi as a speaker but does "
            "not label Elkus Manfredi as the project architect"),
    (88, C, "searched; no civil engineer named"),
    (217, C, "searched; no civil engineer named"),
    (118, A, "searched; MASS Design Group and The Architectural Team appear in a trade "
             "summary but the source page returned 403 and could not be quoted"),
    (118, C, "searched; no civil engineer named"),
    (139, A, "searched; Stantec appears only as a rendering credit, which does not "
             "label the role"),
    (139, C, "searched; no civil engineer named"),
    (431, C, "searched; no civil engineer named"),
    (431, G, "searched; no contractor named"),
    (393, A, "searched; CBT appears in a trade summary but the developer's page could "
             "not be quoted to confirm the role"),
    (393, C, "searched; no civil engineer named"),
    (292, A, "searched; Morris Adjmi, Jaklitsch/Gardner and Gensler appear in a trade "
             "summary but the source page returned 403 and could not be quoted"),
    (292, C, "searched; no civil engineer named"),
    (31, A, "searched; PCA and DREAM Collaborative both appear; the project team page "
            "returned 403 so neither could be tied to this record"),
    (31, C, "searched; Nitsch appears in a trade summary but could not be quoted"),
    (280, A, "searched; DHK's own project page does not state its role in prose"),
    (280, C, "searched; no civil engineer named"),
    (375, A, "searched; sources cover the 22 Boston Wharf vertical addition without "
             "naming a designer"),
    (375, C, "searched; no civil engineer named"),
    (183, C, "searched; no civil engineer named"),
    (89, C, "searched; H+O is structural, not civil; no civil engineer named"),
    (369, C, "searched; no civil engineer named"),
    (388, C, "searched; no civil engineer named"),
    (166, C, "searched; no civil engineer named"),
    (437, C, "searched; no civil engineer named"),
    (95, C, "searched; no civil engineer named"),
    (383, C, "searched; no civil engineer named"),
    (339, C, "searched; R.W. Sullivan named as MEP, not civil"),
    (339, G, "searched; no contractor named"),
    (102, C, "searched; no civil engineer named"),
    (241, C, "searched; no civil engineer named"),
    (387, C, "searched; no civil engineer named"),
    (387, G, "searched; no contractor named"),
    (175, C, "searched; DeSimone named as structural, not civil"),
    (267, A, "searched; Sasaki is master planner and Stantec's page could not be "
             "quoted; neither labelled architect for this phase"),
    (391, C, "searched; no civil engineer named"),
    (408, G, "searched; no contractor named for the remaining master plan"),
    (428, C, "searched; no civil engineer named"),
    (163, G, "searched; Consigli and Smoot named in one summary but not tied to this "
             "record by a quotable source"),
]


def main():
    c = connect()
    rows = {r["id"]: r for r in c.execute(
        "select id,status,completion_stage from projects")}
    made, skipped = 0, 0
    for pid, field, reason in UNRESOLVED:
        live = c.execute("select * from field_provenance where project_id=? and field=? "
                         "and superseded=0", (pid, field)).fetchone()
        if live is not None and live["outcome"] == "resolved":
            skipped += 1
            continue
        r = rows.get(pid)
        outcome = "null"
        if field == G and r is not None and not construction_started(r):
            outcome = "not_yet_selected"
        record(c, pid, field, value=None, outcome=outcome, source_type="web",
               resolution_step=4, reason=reason)
        made += 1
    c.commit()
    print(f"recorded {made} searched-but-unresolved fields ({skipped} already resolved)")


if __name__ == "__main__":
    main()
