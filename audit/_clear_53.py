"""Clear the delivered status on 244-284 A Street.

The only one of the fourteen re-examined range matches that cannot be confirmed.
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

NOTE = (
    " | DELIVERED STATUS CLEARED, UNVERIFIED. This 1,098,292 GSF project was marked "
    "Complete on Certificate of Occupancy COO1300641, which is at 288-304 A STREET "
    "while the project is 244-284 A STREET. The two address ranges DO NOT OVERLAP, and "
    "that test separates cleanly: all nine cross-street errors found earlier had "
    "non-overlapping ranges, and 13 of the 14 range matches re-examined here do "
    "overlap. This one does not. THE PERMIT ALSO DOES NOT DESCRIBE THIS PROJECT: it is "
    "worktype INTREN, described as Renovations - Interior NSC, declared valuation "
    "$4,525,000 -- about $4 per square foot of a project this size, where new "
    "construction in Boston runs $300 to $800. It is an interior fit-out in a "
    "neighbouring building. DISTANCE WAS DELIBERATELY NOT USED to judge this: the "
    "permit is 45 m away, and the nine known-bad cross-street matches were 26 to 101 m "
    "away, so proximity does not discriminate between a match and a neighbour. No "
    "claim is made about this project's real stage."
)

conn = engine.connect()
conn.execute(text("""
    update projects
       set completion_stage = null, completion_basis = null, completion_date = null,
           delivered_date = null, delivered_precision = null, is_flagged = 1,
           notes = coalesce(notes,'') || :n
     where id = 53"""), {"n": NOTE})
conn.commit()
r = conn.execute(text(
    "select id,name,status,completion_stage,delivered_date,total_gsf "
    "from projects where id=53")).first()
print(f"id={r[0]}  {r[1]}  status={r[2]}  stage={r[3]}  delivered={r[4]}  "
      f"GSF={r[5]:,}")
conn.close()
