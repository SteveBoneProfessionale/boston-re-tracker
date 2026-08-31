"""Clear the delivered status on co_issued rows whose CO is not a building completion.

THE RATIO ONLY SAID WHICH ROWS TO READ. The call on each is made from the
permit's own `comments` field, which records what the certificate was actually
issued for. Boston issues a Certificate of Occupancy per TENANT SPACE as well as
per building, and the comments distinguish them explicitly -- "Certificate For
Dental Office-Only", "COO for Dunkin Donuts", "Temporary Certificate for the
Hotel ONLY... Pending Completion".

I looked for rows to keep. There are none in this set, and that is not a failure
to look: the valuation ratio selects for fragments, because a certificate
covering one tenant space carries that tenant's fit-out cost. 33-61 Temple
Street, the control the ratio was supposed to protect, never entered this set --
its CO is valued at $74,145,000, or $431/SF, and it stays untouched.

    python audit/_clear_31.py            # dry run
    python audit/_clear_31.py --apply
"""
import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

# id -> the phrase in the permit's own comments that decides it
CALLS = {
    275: "Temporary for Retail Space - To Expire 10/21/2026 -- a temporary certificate for a retail unit in an 18-storey building",
    353: "Certificate for Store Ground Floor at 200 Stuart Street -- one ground-floor shop, and at a different street number",
    243: "Temporary Certificate for the Hotel ONLY. Expires on 9/3/2026 - Pending Completion of Permits -- the permit says on its face that the work is not finished",
    314: "Four Dwelling Units, Certificate for the Same -- four units against a project of 426",
    240: "Certificate for Amenity Space on the Basement",
    339: "Temporary Certificate for Adult Education Center -- against a project of 358 homes",
    387: "Certificate for Combine Units 6A and 6B into (1) on 6th floor -- two flats merged",
    375: "Certificate For Dental Office-Only",
    239: "Wholesale Warehouse with Accessory Retail and Deli Counter -- the existing warehouse use, not the 151 homes proposed",
    297: "Light Manufacturing (Winery), Restaurant with Function Hall -- not the 96 homes proposed",
    209: "Certificate for Pharmacy @ 32",
    279: "a food-court and restaurant schedule; and the SAME permit COO1808688 is also the completion evidence on id 50, 257 Washington Street -- one certificate matched to two different projects",
    280: "Certificate for Bank",
    383: "COO for Dunkin Donuts -- a 1,200 sq ft donut shop against a project of 170 homes",
    6:   "Certificate for Office Suite 2nd Floor at 225 Friend Street only",
    177: "Certificate for Offices, Lobby and Reception Area - 4th Floor",
    4:   "Certificate for Retail Store for Religious Goods -- against a project of 165 homes",
    368: "Certificate for the Accessory Business Offices of an auto-parts warehouse",
    257: "7 Affordable Residential Units -- against a project of 79",
    305: "Certificate for Private Club Serving Alcohol for Members Only",
    271: "Certificate for Fume Hoods in the Lab Area and Accessory Salon",
    50:  "a food-court and restaurant schedule; and the SAME permit COO1808688 is also the completion evidence on id 279, St. Elizabeth's Medical Center",
    221: "Four (4) Residential Units, Certificate for the Same -- four units against a project of 42",
    255: "Certificate for Physical Therapy office",
    28:  "Certificate For Gym -Only",
    129: "Certificate for Office/Retail at Space at 41 Warren Street -- one unit, at a different street number",
    352: "Certificate for Restaurant/Bakery",
    367: "Repair Service Garage with Storage for Vehicles, Existing Condition NO WORK AT THIS TIME -- the permit states no work was done",
    196: "Certificate for Barber Shop -- against a project of 18 homes",
    365: "(3) Family Dwelling -- three units against a project of 22",
}

PREFIX = (
    " | DELIVERED STATUS CLEARED, UNVERIFIED. The Certificate of Occupancy behind this "
    "row is not a building completion. Boston issues a CO per TENANT SPACE as well as "
    "per building, and this permit's own comments read: "
)
SUFFIX = (
    " A partial or temporary certificate for one space inside a building says nothing "
    "about whether the proposed project was built. NO CLAIM is made about the real "
    "stage -- the row reverts to the status its BPDA page states and is flagged for "
    "verification. Found by reading the permit record, after the declared valuation "
    "flagged the row: this CO's valuation was a small fraction of what a project of "
    "this size costs to build."
)


def main(dry):
    conn = engine.connect()
    n, gsf = 0, 0
    for pid, why in sorted(CALLS.items(),
                           key=lambda kv: -(conn.execute(text(
                               "select coalesce(total_gsf,0) from projects where id=:i"),
                               {"i": kv[0]}).scalar() or 0)):
        r = conn.execute(text(
            "select name,status,completion_stage,total_gsf from projects where id=:i"),
            {"i": pid}).first()
        if r[2] != "Complete":
            print(f"  id={pid:<5}{str(r[0])[:30]:<32}already cleared, skipped")
            continue
        print(f"  id={pid:<5}{str(r[0])[:30]:<32}{int(r[3] or 0):>10,}  "
              f"Complete -> status '{r[1]}'")
        n += 1
        gsf += int(r[3] or 0)
        if dry:
            continue
        conn.execute(text("""
            update projects
               set completion_stage = null, completion_basis = null,
                   completion_date = null, delivered_date = null,
                   delivered_precision = null, is_flagged = 1,
                   notes = coalesce(notes,'') || :n
             where id = :i"""),
            {"i": pid, "n": PREFIX + '"' + why + '".' + SUFFIX})
    if not dry:
        conn.commit()
    print(f"\n{n} rows cleared, {gsf:,} GSF returned to the pipeline")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry=not ap.parse_args().apply)
