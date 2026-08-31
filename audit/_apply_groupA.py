"""Apply the verified Group A fields, the Kinsley merge, and the MXD re-model.

    python audit/_apply_groupA.py            # dry run
    python audit/_apply_groupA.py --apply
"""
import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine, init_db

ROOT = pathlib.Path(__file__).parent
LIVE = json.loads((ROOT / "_groupA_live.json").read_text(encoding="utf-8"))
DATE = "2026-08-31"

# id -> (developer, how the name was established)
PROPONENTS = {
    406: ("Trammell Crow Company / The Community Builders",
          "BPDA page description names Trammell Crow as the Letter of Intent "
          "proponent; Connect CRE and Universal Hub (January 2023) name The "
          "Community Builders as the affordable-housing partner."),
    910: ("Dana-Farber Cancer Institute / Beth Israel Deaconess Medical Center",
          "boston.gov Planning Department approval release and the filing summary: "
          "a joint inpatient cancer hospital on the Joslin Diabetes Center site."),
    405: ("BioMed Realty",
          "The Boston Sun, March and August 2023: BioMed Realty converting the "
          "existing 11-storey building to life-science use, adjacent to 321 "
          "Harrison Avenue which it had already converted."),
    403: ("Cargo Ventures (Horizon/McClellan LLC and CV 580 Chelsea LLC)",
          "The PNF names Horizon/McClellan LLC and CV 580 Chelsea LLC as the "
          "Proponent, filed 9 July 2026; Boston Globe, 10 July 2026, identifies "
          "them as Cargo Ventures affiliates."),
    909: ("Rogerson Communities (for the Charles H. Farnsworth Housing Corporation)",
          "Universal Hub and BLDUP, 2026: Rogerson Communities would build and "
          "manage roughly 114 affordable over-62 apartments for the Farnsworth "
          "Housing Corporation, which bought the 2.6-acre site in February 2025. "
          "SUPERSEDES an earlier 2018 Letter of Intent by Alinea Capital Partners."),
    400: ("Mission Main Tenant Task Force / WinnDevelopment",
          "Universal Hub, threadCRE and Boston Business Journal: the tenant task "
          "force is the proponent with WinnDevelopment as developer and minority "
          "partner, under the BHA Faircloth-to-RAD initiative."),
    904: ("Beacon Communities",
          "Universal Hub 2026 and Boston Globe, November 2022: Beacon converting "
          "the vacant eight-storey building to roughly 47 income-restricted "
          "apartments. Oxford Properties held the ground lease and pledged its "
          "leasehold to the Asian CDC as part of its 125 Lincoln Street deal."),
    402: ("Ivan Biesty",
          "Universal Hub, BLDUP and Bulletin Newspapers, 2026: Biesty bought the "
          "site for $2.5M in July 2025 and proposes 44 units over two combined "
          "parcels."),
    399: ("WC-Ambojang LLC",
          "Boston Planning Department post and Allstonia citing the Small Project "
          "Review Application filed 26-27 June 2026, which names WC-Ambojang LLC "
          "as owner entity and Sousa Design Architects as architect."),
    908: ("JPlain Development LLC",
          "Jamaica Plain News, July 2026: JPlain Development LLC, led by Frank "
          "Schillace and Thomas Niedermeyer."),
    907: ("Colm O'Shea and Patrick Maloney",
          "Jamaica Plain News, August 2026, and Universal Hub: O'Shea and Maloney "
          "of Hyde Park; an earlier 2020 iteration was presented by Jay Walsh."),
    397: ("Adam Burns (Boston Pinnacle Properties)",
          "Universal Hub 2026: converting the federally owned, National Park "
          "Service-controlled Easton Building under the office-to-residential "
          "pilot. NPS issued an RFP for the building on 27 May 2025."),
    906: ("Adnan Salam",
          "Dorchester Reporter, 13 August 2026: a 16-unit condominium project "
          "across 25 Tolman Street, 25R Tolman Street and 995 Morrissey Boulevard."),
    903: ("LSI Inc. (Language Studies International)",
          "Banker & Tradesman: converting the five-storey building to 15 "
          "apartments under the office-to-residential pilot, roughly $4M."),
    396: ("HYM Investment Group / My City at Peace",
          "NEREJ, WBUR, Dorchester Reporter and Banker & Tradesman, July 2026: a "
          "joint venture of HYM (Thomas O'Brien) and My City at Peace (Rev. "
          "Jeffrey Brown), on a site owned by Silver Carney Dorchester LLC, an "
          "Apollo Global Management affiliate. Architect CBT. Carney Hospital "
          "closed in 2024 in the Steward Health Care collapse."),
    398: ("Midwood Investment and Development",
          "Boston Globe, June 2026, and Universal Hub: Midwood (CEO John Usdan) "
          "filed the Letter of Intent. Midwood also proposes a 760-unit tower at "
          "11-21 Bromfield Street."),
}

# GSF taken from the page DESCRIPTION where the structured field publishes none.
FROM_DESC = {
    396: (930000, 'BPDA page description: "a five-building program comprising '
                  'approximately 930,000 square feet of gross floor area" -- 290,000 '
                  'healthcare, 220,000 senior housing (200 units), 320,000 '
                  'multifamily (300 units), 50,000 education, 50,000 retail. The '
                  'structured Gross Floor Area field publishes nothing. NOTE this '
                  'differs from the 970,000 SF reported by Banker & Tradesman; the '
                  'BPDA page is Tier 1 and is taken as current.'),
    398: (97000, 'BPDA page description: "a 155 foot tall, approximately 97,000 gross '
                 'square feet building containing approximately 3,500 square feet of '
                 'retail uses on the ground floor, and residential uses on its upper '
                 '16 stories... approximately 158 rental apartments". The structured '
                 'field publishes nothing.'),
}

MERGE_KEEP, MERGE_DROP = 702, 824


def main(dry):
    init_db()
    conn = engine.connect()

    print("=== Group A: proponent, GSF, land ===")
    for pid in sorted(LIVE, key=lambda k: -(LIVE[k]["stored"] or 0)):
        v = LIVE[pid]
        pid_i = int(pid)
        dev, why = PROPONENTS.get(pid_i, (None, None))
        gsf = v["live_gfa"] or None
        src = "bpda_page"
        if pid_i in FROM_DESC:
            gsf, dsc = FROM_DESC[pid_i]
            src = "bpda_page_description"
        print(f"  id={pid_i:<5}{str(v['name'])[:30]:<32}gsf={str(gsf or '-'):>9}  "
              f"land={str(v['land'] or '-'):>8}  dev={str(dev)[:36]}")
        if dry:
            continue
        note = (f" | VERIFIED {DATE} AGAINST THE BPDA PROJECT PAGE, fetched live. "
                f"Gross Floor Area field reads {v['live_gfa']:,}; Land Sq. Feet reads "
                f"{v['land']:,}. " if v["live_gfa"] and v["land"] else
                f" | VERIFIED {DATE} against the BPDA project page, fetched live. ")
        if pid_i in FROM_DESC:
            note += FROM_DESC[pid_i][1] + " "
        if why:
            note += ("DEVELOPER FIELD WAS BLANK and is now filled: " + dev +
                     ". Basis: " + why + " This is corroborating-tier evidence, not a "
                     "filing, so the resolution method is recorded as web_corroborated.")
        sets = ["notes = coalesce(notes,'') || :n",
                "requires_extraction = 0"]
        params = {"i": pid_i, "n": note}
        if gsf:
            sets += ["total_gsf = :g", "total_gsf_source = :s"]
            params["g"] = int(gsf)
            params["s"] = src
        if v["land"]:
            sets.append("land_sq_ft = :l")
            params["l"] = int(v["land"])
        if dev:
            sets += ["developer = :d", "developer_canonical = :d",
                     "developer_resolution_method = 'web_corroborated'"]
            params["d"] = dev
        conn.execute(text(f"update projects set {', '.join(sets)} where id = :i"),
                     params)

    print("\n=== merge 288 Kinsley Ave ===")
    print(f"  keep id={MERGE_KEEP}, retire id={MERGE_DROP} as a duplicate")
    if not dry:
        conn.execute(text("""
            update projects
               set developer = 'Procaccianti Companies (filed as OGN, LLC)',
                   developer_canonical = 'Procaccianti Companies',
                   developer_resolution_method = 'web_corroborated',
                   notes = coalesce(notes,'') || :n
             where id = :i"""), {"i": MERGE_KEEP, "n":
            " | MERGED RECORD. ecoRI News, 18 November 2020: a single proposal by "
            "PROCACCIANTI COMPANIES, filed under the entity OGN LLC -- an eight-pump "
            "gas station, a five-storey self-storage building and a convenience store "
            "with delicatessen and drive-thru on a vacant 3.8-acre lot at Dean Street "
            "and Kinsley Avenue -- was DENIED by the Providence City Plan Commission on "
            "17 November 2020, citing conflicts with the Great Streets Initiative, the "
            "Urban Trail Network and the Woonasquatucket River Greenway. Record id 824 "
            "carried the same address and the same 175,416 GSF under the name "
            "Procaccianti Companies and is the SAME APPLICATION entered twice under the "
            "developer name and the filing-entity name; it is retired as a duplicate of "
            "this row. THE DENIAL ITSELF IS TIER 3 PRESS and the status is unchanged."})
        conn.execute(text("""
            update projects
               set excluded = 1, out_of_scope = null, is_flagged = 1,
                   excluded_reason = :r, notes = coalesce(notes,'') || :n
             where id = :i"""), {"i": MERGE_DROP,
            "r": f"Duplicate of project id {MERGE_KEEP} (288 Kinsley Ave, Providence)",
            "n": " | RETIRED AS A DUPLICATE of id 702. Same address, same 175,416 GSF, "
                 "same site, both Denied. ecoRI News (18 November 2020) describes ONE "
                 "proposal: Procaccianti Companies filing under the entity OGN LLC. The "
                 "two rows are the developer name and the filing-entity name for a "
                 "single application, entered from two Providence case files (26-234 "
                 "and 27-307). Row kept, not deleted."})

    print("\n=== re-model MXD Infill as one PB-315 permit ===")
    print("  id=428 250 Binney (Building D)  -> Phase 4 BASELINE, counts")
    print("  id=468 105 Broadway (Building E)-> Phase 4 ALTERNATIVE, contingent")
    if not dry:
        conn.execute(text("""
            update projects
               set phase_group = 'MXD Infill PB-315 Phase 4',
                   conditional_alternative = 0,
                   notes = coalesce(notes,'') || :n
             where id = 428"""), {"n":
            " | RE-MODELLED AS THE PHASE 4 BASELINE of a single special permit. The BXP "
            "MXD IDCP Amendment #3 narrative (February 2025, filed with the Cambridge "
            "CDD and the Cambridge Redevelopment Authority) calls 250 Binney "
            "'Commercial Building D', the 'Phase 4 Baseline', with net-new GFA of "
            "372,822 SF. The Cambridge Development Log (2024 Q1) confirms Total GFA "
            "450,576 SF, use Office/R&D, lot area 60,624 SF, FAR approximately 7.78, "
            "status 'Zoning Permit Granted or As of Right, May 10, 2024', special "
            "permit PB315 MA2. THIS IS NOT A COMPETING DESIGN: it is the default branch "
            "of PB-315 and it counts. Reconstruction is delayed -- unlikely to start "
            "before 2029 because the current occupant holds contractual rights, which "
            "is why BXP filed Amendment #3."})
        conn.execute(text("""
            update projects
               set phase_group = 'MXD Infill PB-315 Phase 4',
                   conditional_alternative = 1,
                   notes = coalesce(notes,'') || :n
             where id = 468"""), {"n":
            " | RE-MODELLED AS THE PHASE 4 ALTERNATIVE of the SAME special permit, not a "
            "separate approved building. Per the BXP MXD IDCP Amendment #3 narrative, "
            "105 Broadway is 'Commercial Building E', the 'Phase 4 Alternative'. It "
            "would REALLOCATE the same 372,822 SF of net-new Phase 4 GFA across two "
            "parcels instead of one -- a smaller Building D at 250 Binney (223,515 SF "
            "net-new) plus Building E here (146,757 SF net-new plus 2,550 SF retail) -- "
            "and takes legal effect ONLY IF BXP submits 75 percent design drawings "
            "under PB-315. Held out of totals so the same square footage is not counted "
            "on two rows. THE STORED 302,400 GSF IS UNCONFIRMED: the authoritative "
            "net-new commercial figure is 146,757 SF, and 302,400 does not appear "
            "verbatim in the record."})

    if not dry:
        conn.commit()
        print("\napplied")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry=not ap.parse_args().apply)
