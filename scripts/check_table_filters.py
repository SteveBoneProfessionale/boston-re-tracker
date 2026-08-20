"""Exercise the screener's two new filters and the column widths.

Replicates the filter chain from app/tabs/project_table.py exactly -- the same
comparisons in the same order -- so the combinations can be checked without a
browser. Streamlit only ever applies these to `filtered`, which every earlier
filter has already narrowed, so composition is what actually needs proving.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.data import load_projects
from app.tabs.project_table import _NOT_SPECIFIED, _team_options, _sort_key

TABLE = Path("app/tabs/project_table.py")


def apply_team(frame, sel, col):
    """The exact logic the tab uses."""
    if sel == "All":
        return frame
    vals = frame[col].fillna("").astype(str).str.strip()
    return frame[vals == ""] if sel == _NOT_SPECIFIED else frame[vals == sel]


def main():
    df = load_projects()
    n = len(df)
    arch_opts = _team_options(df, "architect")
    civ_opts = _team_options(df, "civil_engineer")

    print(f"{n} projects loaded")
    print(f"ARCHITECT dropdown:      All, {_NOT_SPECIFIED}, + {len(arch_opts)} names")
    print(f"CIVIL ENGINEER dropdown: All, {_NOT_SPECIFIED}, + {len(civ_opts)} names")
    print(f"  alphabetised: {arch_opts[:3]} ... {arch_opts[-2:]}")
    print(f"  sorted correctly: {arch_opts == sorted(arch_opts, key=_sort_key)}")
    print(f"  no blanks or sentinels in options: "
          f"{all(o and o != 'not_yet_selected' for o in arch_opts + civ_opts)}")
    print()

    # 1. Each filter alone.
    a_named = arch_opts[0]
    c_named = civ_opts[0]
    print("EACH FILTER ALONE")
    print(f"  ARCHITECT = All                     -> {len(apply_team(df, 'All', 'architect')):3}")
    print(f"  ARCHITECT = {_NOT_SPECIFIED:<22} -> "
          f"{len(apply_team(df, _NOT_SPECIFIED, 'architect')):3}")
    print(f"  ARCHITECT = {a_named[:22]:<22} -> "
          f"{len(apply_team(df, a_named, 'architect')):3}")
    print(f"  CIVIL     = {_NOT_SPECIFIED:<22} -> "
          f"{len(apply_team(df, _NOT_SPECIFIED, 'civil_engineer')):3}")
    print(f"  CIVIL     = {c_named[:22]:<22} -> "
          f"{len(apply_team(df, c_named, 'civil_engineer')):3}")
    print()

    # 2. The two together, in the order the tab applies them.
    print("THE TWO TOGETHER")
    both_missing = apply_team(apply_team(df, _NOT_SPECIFIED, "architect"),
                              _NOT_SPECIFIED, "civil_engineer")
    print(f"  architect missing AND civil missing  -> {len(both_missing):3}")
    have_arch_no_civ = apply_team(apply_team(df, "All", "architect"),
                                  _NOT_SPECIFIED, "civil_engineer")
    have_arch_no_civ = have_arch_no_civ[
        have_arch_no_civ["architect"].fillna("").str.strip() != ""]
    print(f"  has an architect but no civil        -> {len(have_arch_no_civ):3}")

    # A pairing that genuinely co-occurs, to prove an AND rather than an OR.
    pair = df[(df["architect"].fillna("").str.strip() != "")
              & (df["civil_engineer"].fillna("").str.strip() != "")]
    a, cvl = pair.iloc[0]["architect"], pair.iloc[0]["civil_engineer"]
    both = apply_team(apply_team(df, a, "architect"), cvl, "civil_engineer")
    only_a = apply_team(df, a, "architect")
    print(f"  {a[:20]:<20} + {cvl[:20]:<20} -> {len(both):3} "
          f"(architect alone {len(only_a)}) AND-composes: {len(both) <= len(only_a)}")
    print()

    # 3. Combined with the filters that were already there.
    print("COMBINED WITH EXISTING FILTERS")
    for city in ("Boston", "Cambridge", "Providence"):
        base = df[df["city"] == city]
        miss = apply_team(base, _NOT_SPECIFIED, "civil_engineer")
        named = base[base["civil_engineer"].fillna("").str.strip() != ""]
        ok = len(miss) + len(named) == len(base)
        print(f"  {city:<11} n={len(base):3}  civil missing {len(miss):3} + "
              f"named {len(named):3} = {len(base):3}  partitions: {ok}")
    boston_appr = df[(df["city"] == "Boston") & (df["status"] == "Board Approved")]
    ba = apply_team(boston_appr, _NOT_SPECIFIED, "architect")
    print(f"  Boston + Board Approved + architect missing -> {len(ba)} "
          f"of {len(boston_appr)}")
    print()

    # 4. Column widths, read back out of the source so this cannot drift.
    src = TABLE.read_text(encoding="utf-8")
    blk = src[src.index("column_config={"):src.index("    # ── Detail panel")]
    widths = dict(re.findall(r'"([^"]+)":\s*st\.column_config\.\w+\(\s*\n?\s*width=(\d+)',
                             blk))
    widths = {k: int(v) for k, v in widths.items()}
    # Widest rendered value per column that must never clip.
    import pandas as pd
    hard = {
        "SF": f"{int(pd.to_numeric(df['total_gsf'], errors='coerce').max()):,}",
        "UNITS": f"{int(pd.to_numeric(df['residential_units'], errors='coerce').max()):,}",
        "HEIGHT": f"{int(pd.to_numeric(df['building_height_ft'], errors='coerce').max())} ft",
        "DELIVERY": max((str(v) for v in df["expected_delivery"].fillna("")), key=len),
    }
    print("COLUMN WIDTHS  (must-not-clip columns)")
    for col, widest in hard.items():
        # ~8px a digit, ~7px a letter, ~4px a comma, 16px cell padding,
        # 18px for the sort arrow. Deliberately generous.
        est = sum(4 if ch == "," else 8 if ch.isdigit() else 7 for ch in widest) + 34
        w = widths.get(col, 0)
        print(f"  {col:<9} widest {widest:<14} needs ~{est:3}px  set {w:3}px  "
              f"{'OK' if w >= est else 'TOO NARROW'}")
    print(f"\n  columns sized: {len(widths)} of 15   total {sum(widths.values())}px")


if __name__ == "__main__":
    main()
