r"""ACQUISITIONS — commercial property transactions, shaped like the screener.

Two things about this tab are unlike the Projects tab and both are deliberate.

A PARTIAL INTEREST IS NOT A BUILDING SALE. `price` is always what was actually
paid, so a 15% stake contributes its stake price to Most Active Buyers and
never the whole-asset value. Where a source states an implied whole-asset
valuation it is shown in its own column and labelled implied, because it is
derived arithmetic and not a price anyone paid. TYPE is on the face of the
table for the same reason.

THE TAB STATES ITS OWN COVERAGE. Asset sales from a registry are near-complete
by nature; partial interests and entity-level deals are only as good as what
gets reported, and Massachusetts nominee trusts let beneficial interests move
with no deed at all. A totals row that looks complete when a section of the
market is structurally invisible is the failure mode here, so the coverage note
sits above the table rather than in a footnote nobody reads.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import math
import pandas as pd
import streamlit as st

# NOT imported at module level, deliberately. A module-level import here runs
# during `from app.tabs import ... acquisitions` in app/main.py, so anything that
# goes wrong inside it takes down the ENTIRE app -- every tab, not just this one
# -- and Streamlit Cloud redacts the message, leaving a traceback that stops one
# line short of the cause. Importing inside render() means a failure is contained
# to this tab and can be shown in full.

_BG2    = "#141720"
_BORDER = "#1E2530"
_ORANGE = "#F5821E"
_MUTED  = "#8A9BB0"
_UI     = "'Inter', -apple-system, 'Segoe UI', Roboto, sans-serif"
# Monospace is kept ONLY for strings read character by character --
# parcel ids, registry citations, coordinates -- never for prose.
_CODE   = "'IBM Plex Mono', ui-monospace, monospace"
# One mark per transaction type, so a stake never reads as a building.
TYPE_MARK = {
    "asset_sale":       ("●", "ASSET",   "#22c55e"),
    "partial_interest": ("◑", "STAKE",   "#F5821E"),
    "entity_level":     ("◆", "ENTITY",  "#38bdf8"),
    "distressed":       ("▲", "DISTRESS", "#ef4444"),
}

# Where a row came from. Kept on the face of the table because the sources are
# not interchangeable and a licensed feed will eventually have to be reconciled
# against what is already here. An audited 10-Q disposition schedule is exact to
# the day and dollar and beats a deed on price; trade press covers the top of the
# market and is structurally blind below it; an assessment feed is complete but
# states no document type, so it cannot testify to arm's length.
SOURCE_MARK = {
    "sec_filing":        ("▣", "SEC",   "#22c55e"),
    "cambridge_socrata": ("▤", "ASSESS", "#38bdf8"),
    "suffolk_registry":  ("▥", "DEED",  "#22c55e"),
    "press":             ("◇", "PRESS", "#F5821E"),
    "broker_release":    ("◇", "PRESS", "#F5821E"),
}

# Same sizing arithmetic as the screener: 14px cells, 8px padding each side,
# plus the header's sort icon.
#
# THE MEASUREMENT IS NOW AN UPPER BOUND, NOT AN IDENTITY. It used to be exact,
# because the theme font was monospace and every character advanced the same
# 0.6em. Inter is proportional: lowercase runs nearer 0.5em, digits and capitals
# nearer 0.6em. This table is mostly capitals, digits and currency, which is the
# wide end of that range, so the per-character figure is held at 0.6em and the
# slack is widened rather than tightened. The failure that matters is a clipped
# value, not a column with air in it -- the grid ellipsises anything too wide
# for its column and the reader never learns what was cut.
_CHAR_PX, _CELL_PAD, _HEADER_ICON, _SLACK, _MIN = 14.0 * 0.6, 16, 18, 14, 44
_WIDE = 1.6


def _text_px(t) -> float:
    return _CHAR_PX * sum(_WIDE if ord(c) >= 0x2000 else 1.0 for c in str(t))


def _fmt_money(v) -> str:
    return "" if v is None or v != v else f"{int(v):,}"


_CELL = {
    "PRICE":       _fmt_money,
    "SF":          _fmt_money,
    "UNITS":       lambda v: "" if v is None or v != v else f"{int(v)}",
    "$/SF":        lambda v: "" if v is None or v != v else f"{v:,.0f}",
    "$/UNIT":      _fmt_money,
    "PRIOR PRICE": _fmt_money,
    "CHANGE %":    lambda v: "" if v is None or v != v else f"{v:.1f}%",
    "YEAR":        lambda v: "" if v is None or v != v else f"{int(v)}",
    "DATE":        lambda v: "" if v is None or v != v else str(v)[:10],
    "PRIOR DATE":  lambda v: "" if v is None or v != v else str(v)[:10],
}


def _widths(display: pd.DataFrame) -> dict:
    out = {}
    for col in display.columns:
        render = _CELL.get(col, str)
        longest = max((_text_px(render(v)) for v in display[col]), default=0.0)
        header = _text_px(col) + _HEADER_ICON
        out[col] = max(_MIN, math.ceil(max(longest, header) + _CELL_PAD + _SLACK))
    return out


# How a sponsor name was arrived at. Rendered on the face of every canonical
# name, because "Blackstone" read off a deed and "Blackstone" inferred from a
# shared mailing address are not the same claim.
RESOLUTION_MARK = {
    "registry_confirmed": ("▣", "from the record itself"),
    "pattern_matched":    ("◈", "from the entity's naming convention"),
    "web_corroborated":   ("◇", "from a named publication"),
    "human_set":          ("✎", "set by hand"),
}


def _sponsor_cell(canon, conf) -> str:
    if not canon or canon != canon:
        return ""
    mark = RESOLUTION_MARK.get(conf, ("·", "unknown basis"))[0]
    return f"{mark} {canon}"


def _rankings(f: pd.DataFrame):
    """Most active buyers and sellers, on RESOLVED SPONSORS ONLY.

    Never falls back to the record entity. A ranking that mixes "Blackstone"
    with "100 SUMMER OWNER LLC" is not a ranking of firms, it is a ranking of
    two different kinds of thing, and the single-purpose vehicles would each
    appear once and rank nowhere while quietly removing their sponsor's volume
    from the total. Unresolved rows are excluded and the exclusion is stated.
    """
    total_rows = len(f)
    dates = pd.to_datetime(f["sale_date"], errors="coerce").dropna()

    for side, label in (("buyer", "BUYERS"), ("seller", "SELLERS")):
        canon = f.get(f"{side}_canonical")
        if canon is None:
            continue
        # Intra-sponsor conveyances are excluded. A firm moving an asset between
        # its own vehicles is not acquiring anything, and counting it as one
        # distorts the ranking badly: twelve such rows carry $2.70B, of which
        # $2.33B is Alexandria buying from Alexandria -- more than half its
        # apparent buy-side volume.
        arms = f.get("arms_length")
        affiliated = (arms == 0) if arms is not None else pd.Series(False, index=f.index)
        n_affil = int(affiliated.sum())
        v_affil = float(pd.to_numeric(f.loc[affiliated, "price"],
                                      errors="coerce").fillna(0).sum())
        sub = f[(canon.fillna("") != "") & (~affiliated)].copy()
        sub["_who"] = sub[f"{side}_canonical"]
        n_res = len(sub)
        vol_res = float(pd.to_numeric(sub["price"], errors="coerce").fillna(0).sum())
        vol_all = float(pd.to_numeric(f["price"], errors="coerce").fillna(0).sum())

        _section(f"MOST ACTIVE {label}")
        if n_res == 0:
            st.caption("No resolved sponsors in the current filter.")
            continue

        sd = pd.to_datetime(sub["sale_date"], errors="coerce").dropna()
        span = (f"{sd.min():%b %Y} to {sd.max():%b %Y}" if len(sd) else "n/a")
        recent = int((sd >= "2023-01-01").sum()) if len(sd) else 0
        st.markdown(
            f'<div style="font-family:{_UI};font-size:10px;line-height:1.6;'
            f'color:{_MUTED};border-left:2px solid {_ORANGE};padding:6px 10px;'
            f'margin:6px 0;background:{_BG2}">'
            f'<b style="color:#e2e8f0">RESOLUTION</b> {n_res} of {total_rows} rows '
            f'({n_res/total_rows*100:.0f}%) carry a resolved sponsor, covering '
            f'${vol_res/1e9:.2f}B of ${vol_all/1e9:.2f}B '
            f'({vol_res/vol_all*100 if vol_all else 0:.0f}% of dollars). '
            f'Rows whose {side} is still a single-purpose entity are EXCLUDED, '
            f'not bucketed as "other", so this ranks the firms it can name and '
            f'understates everyone. A further {n_affil} rows worth '
            f'${v_affil/1e9:.2f}B are excluded as INTRA-SPONSOR CONVEYANCES — the '
            f'same firm on both sides, which is restructuring, not acquisition.<br>'
            f'<b style="color:#e2e8f0">DATE COVERAGE</b> {span}. Only {recent} of '
            f'these {n_res} rows are dated 2023 or later — the assessment spines '
            f'stop in 2022 (Boston) and 2025 (Cambridge), so this is a '
            f'<b style="color:#e2e8f0">HISTORICAL</b> ranking and must not be read '
            f'as current market share.</div>', unsafe_allow_html=True)

        agg = (sub.groupby("_who")
                  .agg(deals=("id", "count"),
                       dollars=("price", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()))
                  .reset_index())
        c1, c2 = st.columns(2)
        by_v = agg.sort_values("dollars", ascending=False).head(12)
        c1.caption("By dollar volume — a stake contributes its stake price, "
                   "never the implied value of the building it sits in.")
        c1.dataframe(
            by_v.rename(columns={"_who": side.upper(), "deals": "DEALS",
                                 "dollars": "DOLLARS"}),
            use_container_width=True, hide_index=True, height=300,
            column_config={"DOLLARS": st.column_config.NumberColumn(format="$%,d"),
                           "DEALS": st.column_config.NumberColumn(format="%d")})
        by_n = agg.sort_values(["deals", "dollars"], ascending=False).head(12)
        c2.caption("By deal count — a different ranking, because one $435M "
                   "trade and twenty $5M trades are different businesses.")
        c2.dataframe(
            by_n.rename(columns={"_who": side.upper(), "deals": "DEALS",
                                 "dollars": "DOLLARS"}),
            use_container_width=True, hide_index=True, height=300,
            column_config={"DOLLARS": st.column_config.NumberColumn(format="$%,d"),
                           "DEALS": st.column_config.NumberColumn(format="%d")})


def _section(label: str):
    st.markdown(
        f'<p style="font-family:{_UI};font-size:9px;font-weight:700;'
        f'letter-spacing:0.18em;color:{_MUTED};text-transform:uppercase;'
        f'margin:16px 0 8px 0">{label}</p>', unsafe_allow_html=True)


def _asset_cell(raw) -> str:
    """Normalised asset class, or the raw code behind a warning mark.

    An unmapped code is shown AS ITSELF with a ⚠, never bucketed into "Other".
    Four codes are in that state -- Cell Carrier, Com Billboard, Comm Condo and
    the bare word Commercial -- covering 14 rows. "Other" would read as an asset
    class and quietly assert that they are one thing; the raw code asserts only
    what the assessor wrote.
    """
    from app.asset_class import classify
    cat = classify(raw)
    if cat:
        return cat
    if raw is None or raw != raw or not str(raw).strip():
        return ""
    return f"⚠ {raw}"


def _build_display(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    # TYPE IS A MARK ON THE ADDRESS, NOT A COLUMN. 785 of 793 rows are plain
    # asset sales, so a full column spent saying ASSET 785 times carried almost
    # no information. Only the eight rows that are NOT a straightforward whole
    # -property sale are marked, which is the entire signal the column carried.
    # The stake percentage rides along on the same cell, because it is populated
    # on three rows and a column for three values is worse than a suffix.
    def _addr(row):
        t = row["transaction_type"]
        mark = TYPE_MARK.get(t)
        pre = f"{mark[0]} " if mark and t != "asset_sale" else ""
        pct = pd.to_numeric(pd.Series([row.get("pct_acquired")]),
                            errors="coerce").iloc[0]
        suf = f"  ·  {pct:.0f}% stake" if pct == pct and pct else ""
        return f"{pre}{row['address'] or ''}{suf}"

    addr = d.apply(_addr, axis=1) if len(d) else pd.Series(dtype=object)

    d["SRC"] = [f"{SOURCE_MARK.get(s, ('·', (s or '?').upper()[:6], _MUTED))[0]} "
                f"{SOURCE_MARK.get(s, ('·', (s or '?').upper()[:6], _MUTED))[1]}"
                for s in d["source"]]

    dates = pd.to_datetime(d["sale_date"], errors="coerce")

    # A PLACEHOLDER SQUARE FOOTAGE IS NOT A SQUARE FOOTAGE. Rows loaded with
    # building_sf = 1 computed to $452,000,000/SF at 101 Seaport and
    # $285,000,000/SF at 185 Franklin. Those were nulled in the data, and this
    # is the guard that stops any future loader putting one back on screen.
    sf = pd.to_numeric(d["building_sf"], errors="coerce")
    psf = pd.to_numeric(d["price_per_sf"], errors="coerce")
    broken = sf.notna() & (sf <= 1)
    sf = sf.mask(broken)
    psf = psf.mask(broken)

    # THE SAME DEFECT ONE STEP SUBTLER. psf_unreliable marks the rows where the
    # recorded area is the PARCEL's and not the asset's, so the quotient
    # describes nothing. 29 of them still carried a figure, and they separate
    # from the sound data without overlap: every flagged $/SF is $2,516 or more
    # while the highest unflagged one in the table is $2,488. A $72,865/SF cell
    # is the same broken number as a $452,000,000/SF cell, just quieter, so it
    # is withheld on the same rule. THE SQUARE FOOTAGE ITSELF IS KEPT -- the
    # parcel's recorded area is real, it is only the wrong denominator for this
    # asset -- and the row detail says so.
    unreliable = pd.to_numeric(d.get("psf_unreliable"), errors="coerce").fillna(0) == 1
    psf = psf.mask(unreliable)

    # COLUMN ORDER IS THE SENTENCE THE ROW READS AS: what it is, when it
    # happened, who sold, who bought, what it cost, how big, what type. The
    # deal is legible in the first seven columns without a horizontal scroll.
    #
    # CITY, SUBMARKET and YEAR sit to the right of the money on purpose. Each
    # has its own control in the filter bar above, so their job is done before
    # the eye reaches the table; carrying them on the left would push the
    # parties and the price off the first screen to restate what the filter
    # already says.
    out = pd.DataFrame({
        "ADDRESS":  addr,
        # Real dates and real numbers throughout, so every header sorts
        # chronologically or by magnitude rather than lexically. Verified, not
        # assumed -- string-typed numbers already bit the Projects table once.
        "DATE":     dates,
        # SELLER BEFORE BUYER, which reads in the direction the deal happened.
        #
        # THE RESOLVED SPONSOR ONLY. Blank where unresolved.
        #
        # The record entity is deliberately NOT shown. "115 Banker Street LLC
        # sold to 245 Bluefish LLC" occupies two columns to say nothing; the
        # question is always which firm sold to which firm. A blank cell is the
        # honest answer to "who was this" when the vehicle has not been resolved,
        # and it makes the resolution rate visible at a glance instead of hiding
        # it behind a wall of LLC names that look like data.
        #
        # `buyer` and `seller` remain stored verbatim and untouched. They are the
        # key that ties a row to its deed, the thing a registry citation resolves
        # against, what a licensed feed would reconcile to, and what every
        # resolution layer runs on. They stay visible on the row detail so any
        # row can still be traced back to the record.
        "SELLER":   [_sponsor_cell(cn, cf) for cn, cf in
                     zip(d.get("seller_canonical", pd.Series(dtype=object)),
                         d.get("seller_confidence", pd.Series(dtype=object)))],
        "BUYER":    [_sponsor_cell(cn, cf) for cn, cf in
                     zip(d.get("buyer_canonical", pd.Series(dtype=object)),
                         d.get("buyer_confidence", pd.Series(dtype=object)))],
        "PRICE":    pd.to_numeric(d["price"], errors="coerce"),
        "SF":       sf,
        "ASSET":    [_asset_cell(v) for v in d["property_type"]],
        # Derived and secondary measures, then the filter dimensions.
        "$/SF":     psf,
        "UNITS":    pd.to_numeric(d["unit_count"], errors="coerce"),
        "$/UNIT":   pd.to_numeric(d["price_per_unit"], errors="coerce"),
        "CITY":     d["city"].fillna(""),
        "SUBMARKET": d.get("submarket", pd.Series(index=d.index, dtype=object)).fillna(""),
        "YEAR":     dates.dt.year.astype("Int64"),
        # THE BASIS TRADE, ON THE ROW. 18 Tremont Street bought at $102.75M in
        # 2019 and sold at $29.5M is the single most informative fact in this
        # table and it was previously not visible anywhere.
        "PRIOR PRICE": pd.to_numeric(d["prior_sale_price"], errors="coerce"),
        "PRIOR DATE": pd.to_datetime(d["prior_sale_date"], errors="coerce"),
        "CHANGE %": pd.to_numeric(d["basis_change_pct"], errors="coerce"),
        "SRC":      d["SRC"],
    })
    return out


def _fixed(pairs):
    """Label/value lines in a fixed-width face.

    Used only for identifiers -- record entities, registry citations, parcel
    ids -- where the reader is matching characters against another document and
    the label column has to line up. Everything else in this app is
    proportional.
    """
    import html
    rows = "".join(
        f'<div style="display:flex;gap:10px;padding:1px 0">'
        f'<span style="color:{_MUTED};min-width:72px">{html.escape(str(k))}</span>'
        f'<span style="color:#e2e8f0;word-break:break-word">'
        f'{html.escape(str(v))}</span></div>'
        for k, v in pairs)
    st.markdown(
        f'<div style="font-family:{_CODE};font-size:11.5px;line-height:1.7">'
        f'{rows}</div>', unsafe_allow_html=True)


def _row_detail(f: pd.DataFrame, event):
    """Everything the default table no longer carries, for the selected row.

    The columns cut from the face of the table are not deleted, they live here:
    the RAW assessor code behind the normalised asset class, the stake
    percentage and implied whole-asset valuation that are populated on three
    rows and none respectively, the registry book and page, and — most
    importantly — the RECORD ENTITIES. `buyer` and `seller` are stored verbatim
    and never modified, and this is where a row is traced back to its deed.
    """
    rows = getattr(getattr(event, "selection", None), "rows", None) or []
    if not rows:
        st.caption("Select a row for the record entities, the registry "
                   "citation, the raw assessor code and the research note.")
        return
    i = rows[0]
    if i >= len(f):
        return
    r = f.iloc[i]

    _section("ROW DETAIL")
    a, b = st.columns(2)
    with a:
        st.markdown(f"**{r.get('address') or ''}** — {r.get('city') or ''}"
                    f"{', ' + r['submarket'] if r.get('submarket') else ''}")
        st.caption("RECORD ENTITIES, verbatim and never modified. These are the "
                   "key that ties the row to its deed.")
        bk, pg = r.get("deed_book") or "", r.get("deed_page") or ""
        cite = f"{bk}/{pg}".strip("/") or "—"
        if r.get("is_registered_land"):
            cite += f"  ·  registered land, cert {r.get('certificate_number') or '—'}"
        # THE ONE PLACE MONOSPACE IS STILL CORRECT. Everything here is a string
        # read character by character rather than a word read at a glance -- the
        # grantor and grantee exactly as they appear on the deed, the registry
        # book and page, the parcel identifier. Those are compared digit by
        # digit against a registry, and the label column only lines up in a face
        # where every character advances the same width.
        _fixed([("grantor", r.get("seller") or "—"),
                ("grantee", r.get("buyer") or "—"),
                ("book/pg", cite),
                ("parcel", r.get("parcel_id") or "—")])
    with b:
        st.caption("The assessor's own code, kept so the normalised asset "
                   "class stays traceable.")
        pct = r.get("pct_acquired")
        iv = r.get("implied_valuation")
        _fixed([("raw type", r.get("property_type") or "—"),
                ("stake", f"{pct:.0f}%" if pct == pct and pct else "—"),
                ("implied", f"${iv:,.0f}" if iv == iv and iv else "—"),
                ("source", f"{r.get('source') or '—'}  "
                           f"{r.get('source_date') or ''}".strip())])
        if r.get("source_url"):
            st.markdown(f"[source link]({r['source_url']})")

    for flag, msg in (
            ("psf_unreliable",
             "$/SF IS NOT RELIABLE ON THIS ROW. The recorded square footage is "
             "the parcel's, not the asset's."),
            ("price_disputed",
             "PRICE IS DISPUTED. The recorded consideration and the reported "
             "price do not reconcile."),
    ):
        if r.get(flag):
            st.warning(msg)
    if r.get("price_caveat"):
        st.caption(f"PRICE CAVEAT — {r['price_caveat']}")
    if r.get("notes"):
        with st.expander("Research note"):
            st.write(r["notes"])


def render(projects=None):
    try:
        # app.acq_data, not app.data. The deploy carried a STALE app/data.py --
        # importing load_projects from it worked while load_transactions, defined
        # at column zero in the same file on GitHub, was absent. A module the
        # deployment has never seen has no older copy to serve, so this routes
        # around a wedged checkout instead of hoping the next pull fixes it.
        # app/data.py keeps its own copy, so other callers are unaffected.
        try:
            from app.acq_data import load_transactions
        except ImportError:
            from app.data import load_transactions
        df = load_transactions()
    except Exception as exc:
        import traceback
        st.error(
            "The Acquisitions tab could not load its data. The rest of the app "
            "is unaffected. Full error below — this is the message Streamlit "
            "Cloud redacts."
        )
        st.code(f"{type(exc).__name__}: {exc}", language="text")
        st.code(traceback.format_exc(), language="text")
        with st.expander("Environment"):
            import sqlalchemy
            st.code(
                f"python     {sys.version}\n"
                f"streamlit  {st.__version__}\n"
                f"pandas     {pd.__version__}\n"
                f"sqlalchemy {sqlalchemy.__version__}\n"
                f"cwd        {Path.cwd()}\n"
                f"sys.path[:5]\n  " + "\n  ".join(sys.path[:5]),
                language="text")
        return
    if df.empty:
        st.info("No transactions loaded yet.")
        return

    # ── Coverage, stated before any total is shown ──────────────────
    n_part = df["transaction_type"].isin(["partial_interest", "entity_level"]).sum()
    y26 = df[pd.to_datetime(df["sale_date"], errors="coerce").dt.year == 2026]
    n26 = len(y26)
    n26_priced = int(y26["price"].notna().sum())
    st.markdown(
        f'<div style="font-family:{_UI};font-size:10px;line-height:1.6;color:{_MUTED};'
        f'border-left:2px solid {_ORANGE};padding:8px 12px;margin:10px 0;background:{_BG2}">'
        f'<b style="color:#e2e8f0">COVERAGE — READ THIS BEFORE ANY TOTAL</b><br>'
        f'<b style="color:#e2e8f0">There is no deed feed.</b> masslandrecords sits behind an '
        f'active bot block on both Suffolk and Middlesex South, so the registry spine that '
        f'would make asset-sale coverage near-complete does not exist here. Cambridge\'s '
        f'assessment file carries no sale later than 6 August 2025 and zero 2026 sales, so it '
        f'cannot fill the gap either.<br>'
        f'<b style="color:#e2e8f0">2026 is therefore press- and SEC-sourced only</b> — '
        f'{n26} transactions, {n26_priced} with a stated price. That is the top of the market, '
        f'not the market. Trade press reports large and newsworthy deals; it does not report '
        f'the $2–20M range systematically, so the count is a floor and the dollar total '
        f'understates volume by an unknown amount. Rows marked ◇ PRESS carry that limit; '
        f'▣ SEC rows come from audited disposition schedules and are exact.<br>'
        f'<b style="color:#e2e8f0">The window is 2025 onward, and 2025 has ONE row.</b> '
        f'847 Massachusetts Avenue, Cambridge, $3.4M. That is not a filter artefact: the '
        f'Cambridge source contains exactly one commercial sale at or above the $2M floor for '
        f'the whole of 2025, and Boston\'s state parcel layer stops in October 2022. So this '
        f'table is effectively an eight-month view of 2026, and any per-SF or per-unit '
        f'comparison has no historical baseline behind it inside this window.<br>'
        f'<b style="color:#e2e8f0">A partial interest leaves no deed at all.</b> A '
        f'Massachusetts nominee trust can move beneficial interests with title unchanged, so '
        f'no registry would surface it even with full access. {n_part} such transactions are '
        f'tracked — a floor, and one that a licensed feed would not raise.'
        f'</div>', unsafe_allow_html=True)

    # ── Filters ─────────────────────────────────────────────────────
    _section("FILTER")
    c1, c2, c3, c4 = st.columns(4)
    cities = ["All"] + sorted(x for x in df["city"].dropna().unique() if x)
    city = c1.selectbox("CITY", cities, key="acq_city")
    types = ["All"] + sorted(x for x in df["transaction_type"].dropna().unique() if x)
    ttype = c2.selectbox("TYPE", types, key="acq_type",
                         help="A stake is never a building. STAKE and ENTITY rows "
                              "carry the amount paid for the interest, not the "
                              "whole-asset value.")
    # Filters on the NORMALISED class, not the raw code. Ten options instead of
    # 115, and "Office" now means the same thing in both cities.
    from app.asset_class import CATEGORIES
    present = {_asset_cell(v) for v in df["property_type"].dropna().unique()}
    assets = (["All"] + [c for c in CATEGORIES if c in present]
              + sorted(x for x in present if x.startswith("⚠")))
    asset = c3.selectbox("ASSET CLASS", assets, key="acq_asset")
    years = ["All"] + sorted({str(y)[:4] for y in df["sale_date"].dropna()}, reverse=True)
    year = c4.selectbox("YEAR", years, key="acq_year")

    c5, c6, c7 = st.columns([1, 1, 2])
    subs = ["All"] + sorted(x for x in df.get(
        "submarket", pd.Series(dtype=object)).dropna().unique() if x)
    sub_sel = c5.selectbox("SUBMARKET", subs, key="acq_sub",
                           help="BPDA neighborhood in Boston, CDD neighborhood "
                                "in Cambridge — the same vocabulary the "
                                "Projects tab uses. Derived by point-in-polygon "
                                "from the parcel; blank on the 11 rows that "
                                "have no locatable point.")
    lo = c6.number_input("MIN PRICE ($M)", value=0.0, step=1.0, key="acq_lo")
    hi = c7.number_input("MAX PRICE ($M)", value=0.0, step=1.0, key="acq_hi",
                         help="0 means no ceiling.")

    c8, c9 = st.columns([3, 2])
    q = c8.text_input("SEARCH", "", key="acq_q", placeholder="address, buyer or seller…")
    # THE SET WORTH SHOWING SOMEONE. Buyer is resolved on 37% of rows and seller
    # on 26%, so most of the table is blank on the two columns a CRE reader looks
    # at first. This isolates the rows where both sides are named in one click.
    both = c9.checkbox("BOTH PARTIES NAMED", value=False, key="acq_both",
                       help="Only rows where the buyer AND the seller have been "
                            "resolved to a firm. 185 rows carrying $28.4B — a "
                            "quarter of the table by count and three quarters "
                            "of it by dollar, because resolution clusters at "
                            "the top of the market.")

    f = df.copy()
    if city != "All":
        f = f[f["city"] == city]
    if ttype != "All":
        f = f[f["transaction_type"] == ttype]
    if asset != "All":
        f = f[[_asset_cell(v) == asset for v in f["property_type"]]]
    if sub_sel != "All":
        f = f[f.get("submarket", pd.Series(index=f.index, dtype=object)) == sub_sel]
    if both:
        f = f[(f.get("buyer_canonical", pd.Series(index=f.index, dtype=object))
               .fillna("") != "")
              & (f.get("seller_canonical", pd.Series(index=f.index, dtype=object))
                 .fillna("") != "")]
    if year != "All":
        f = f[f["sale_date"].astype(str).str[:4] == year]
    if lo:
        f = f[f["price"].fillna(0) >= lo * 1e6]
    if hi:
        f = f[f["price"].fillna(0) <= hi * 1e6]
    if q:
        ql = q.lower()
        f = f[f["address"].fillna("").str.lower().str.contains(ql)
              | f["buyer"].fillna("").str.lower().str.contains(ql)
              | f["seller"].fillna("").str.lower().str.contains(ql)]

    vol = int(f["price"].fillna(0).sum())
    st.markdown(
        f'<p style="font-family:{_UI};font-size:10px;color:{_MUTED};margin:4px 0 8px">'
        f'<span style="color:#e2e8f0;font-weight:700">{len(f)}</span> TRANSACTIONS'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;<span style="color:#e2e8f0;font-weight:700">'
        f'${vol/1e9:.2f}B</span> PAID'
        + "".join(f'&nbsp;&nbsp;·&nbsp;&nbsp;<span style="color:{c}">{m}</span>&nbsp;{lbl}'
                  for m, lbl, c in TYPE_MARK.values())
        + '</p>', unsafe_allow_html=True)

    # ── Table ───────────────────────────────────────────────────────
    _section("TRANSACTIONS")
    # DEFAULT SORT IS PRICE DESCENDING. Most rows are blank on buyer and seller,
    # and the named ones cluster at the top of the market, so sorting by price
    # puts the usable data on the first screen instead of whatever order the
    # loader happened to write.
    f = f.sort_values("price", ascending=False, na_position="last")
    disp = _build_display(f)
    w = _widths(disp)
    event = st.dataframe(
        disp, use_container_width=True, hide_index=True, height=420,
        on_select="rerun", selection_mode="single-row", key="acq_table",
        column_config={
            "ADDRESS": st.column_config.TextColumn(
                width=w["ADDRESS"], pinned=True,
                help="◑ STAKE a percentage of the owning entity · ◆ ENTITY the "
                     "owning entity itself · ▲ DISTRESS foreclosure or deed in "
                     "lieu, kept rather than dropped. An unmarked row is a "
                     "straightforward whole-property sale, which is 785 of 793."),
            "YEAR":    st.column_config.NumberColumn(
                width=w["YEAR"], format="%d",
                help="The sale year on its own, for grouping and charting. "
                     "Sort on DATE instead when the order within a year "
                     "matters."),
            "CITY":    st.column_config.TextColumn(width=w["CITY"]),
            "SUBMARKET": st.column_config.TextColumn(
                width=w["SUBMARKET"],
                help="BPDA neighborhood in Boston, Cambridge CDD neighborhood in "
                     "Cambridge — the same vocabulary the Projects tab uses. "
                     "Derived by point-in-polygon from the parcel centroid, not "
                     "from the street name. Blank where the row has no locatable "
                     "point."),
            "PRIOR PRICE": st.column_config.NumberColumn(
                width=w["PRIOR PRICE"], format="$%,d",
                help="What the seller paid for the same asset, where a prior "
                     "trade has been established. 11 rows carry one."),
            "PRIOR DATE": st.column_config.DateColumn(
                width=w["PRIOR DATE"], format="YYYY-MM-DD",
                help="When the prior trade closed. CHANGE % is meaningless "
                     "without it — a 71% fall over three years and over "
                     "fifteen are different events."),
            "CHANGE %": st.column_config.NumberColumn(
                width=w["CHANGE %"], format="%.1f%%",
                help="Change against the seller's own basis. 18 Tremont Street "
                     "at −71.3% and 1 Hampshire Street at −62.5% are the "
                     "repricing of Boston office in the open."),
            "SRC":     st.column_config.TextColumn(
                width=w["SRC"],
                help="Where the row came from. ▣ SEC an audited 10-Q disposition "
                     "schedule, exact to the day and dollar. ▤ ASSESS a municipal "
                     "assessment feed, complete but silent on document type. "
                     "▥ DEED a registry record. ◇ PRESS a named publication "
                     "reporting a named price — reliable at the top of the market, "
                     "structurally blind below it. Kept visible so a licensed feed "
                     "can be reconciled against what is already here."),
            "DATE":    st.column_config.DateColumn(width=w["DATE"], format="YYYY-MM-DD"),
            "PRICE":   st.column_config.NumberColumn(
                width=w["PRICE"], format="$%,d",
                help="What was actually PAID. On a stake this is the stake price, "
                     "never the whole-asset value."),
            "BUYER":   st.column_config.TextColumn(
                width=w["BUYER"],
                help="The RESOLVED SPONSOR, not the record entity. Blank means "
                     "the single-purpose vehicle on the deed has not been "
                     "resolved to a firm — a blank is the honest answer, not a "
                     "missing value. The record entity is stored verbatim and "
                     "shown on the row detail."),
            "SELLER":  st.column_config.TextColumn(
                width=w["SELLER"],
                help="The RESOLVED SPONSOR, not the record entity. Blank means "
                     "unresolved. The grantor of record is stored verbatim and "
                     "shown on the row detail."),
            "ASSET":   st.column_config.TextColumn(
                width=w["ASSET"],
                help="Normalised to one taxonomy across both cities. Boston "
                     "records 'Office Cls B+ (346)' and Cambridge records "
                     "'GEN-OFFICE' for the same thing; both read Office here. "
                     "A ⚠ means the assessor's code does not map to any class "
                     "and is shown raw rather than bucketed as Other. The "
                     "original code is on the row detail."),
            "SF":      st.column_config.NumberColumn(width=w["SF"], format="%,d"),
            "UNITS":   st.column_config.NumberColumn(width=w["UNITS"], format="%d"),
            "$/SF":    st.column_config.NumberColumn(width=w["$/SF"], format="$%,d"),
            "$/UNIT":  st.column_config.NumberColumn(width=w["$/UNIT"], format="$%,d"),
        })

    _row_detail(f, event)

    # ── Rankings, on resolved sponsors only ─────────────────────────
    _rankings(f)

    a, b = st.columns(2)
    with a:
        _section("VOLUME BY QUARTER")
        qtr = f.dropna(subset=["sale_date"]).copy()
        if not qtr.empty:
            qtr["Q"] = pd.PeriodIndex(pd.to_datetime(qtr["sale_date"]), freq="Q").astype(str)
            st.bar_chart(qtr.groupby("Q")["price"].sum(), height=200)
    with b:
        _section("MEDIAN $/SF BY ASSET CLASS")
        # On the NORMALISED class. Grouping by the raw code produced 115 bars in
        # two vocabularies and compared nothing to anything.
        pps = f.dropna(subset=["price_per_sf"]).copy()
        pps = pps[pd.to_numeric(pps["building_sf"], errors="coerce").fillna(0) > 1]
        if not pps.empty:
            pps["_cls"] = [_asset_cell(v) for v in pps["property_type"]]
            st.bar_chart(pps.groupby("_cls")["price_per_sf"].median()
                            .sort_values(ascending=False).head(12), height=200)
