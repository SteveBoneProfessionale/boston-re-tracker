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

from app.data import load_transactions

_BG2    = "#141720"
_BORDER = "#1E2530"
_ORANGE = "#F5821E"
_MUTED  = "#8A9BB0"
_MONO   = "'JetBrains Mono', 'IBM Plex Mono', monospace"

# One mark per transaction type, so a stake never reads as a building.
TYPE_MARK = {
    "asset_sale":       ("●", "ASSET",   "#22c55e"),
    "partial_interest": ("◑", "STAKE",   "#F5821E"),
    "entity_level":     ("◆", "ENTITY",  "#38bdf8"),
    "distressed":       ("▲", "DISTRESS", "#ef4444"),
}

# Same sizing arithmetic as the screener: monospace theme font, 14px cells,
# 8px padding each side, plus the header's sort icon.
_CHAR_PX, _CELL_PAD, _HEADER_ICON, _SLACK, _MIN = 14.0 * 0.6, 16, 18, 8, 44
_WIDE = 1.6


def _text_px(t) -> float:
    return _CHAR_PX * sum(_WIDE if ord(c) >= 0x2000 else 1.0 for c in str(t))


def _fmt_money(v) -> str:
    return "" if v is None or v != v else f"{int(v):,}"


_CELL = {
    "PRICE":     _fmt_money,
    "IMPLIED":   _fmt_money,
    "SF":        _fmt_money,
    "UNITS":     lambda v: "" if v is None or v != v else f"{int(v)}",
    "$/SF":      lambda v: "" if v is None or v != v else f"{v:,.0f}",
    "$/UNIT":    _fmt_money,
    "%":         lambda v: "" if v is None or v != v else f"{v:.0f}%",
    "DATE":      lambda v: "" if v is None or v != v else str(v)[:10],
}


def _widths(display: pd.DataFrame) -> dict:
    out = {}
    for col in display.columns:
        render = _CELL.get(col, str)
        longest = max((_text_px(render(v)) for v in display[col]), default=0.0)
        header = _text_px(col) + _HEADER_ICON
        out[col] = max(_MIN, math.ceil(max(longest, header) + _CELL_PAD + _SLACK))
    return out


def _section(label: str):
    st.markdown(
        f'<p style="font-family:{_MONO};font-size:9px;font-weight:700;'
        f'letter-spacing:0.18em;color:{_MUTED};text-transform:uppercase;'
        f'margin:16px 0 8px 0">{label}</p>', unsafe_allow_html=True)


def _build_display(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["TYPE"] = [f"{TYPE_MARK.get(t, ('·', t or '?', _MUTED))[0]} "
                 f"{TYPE_MARK.get(t, ('·', (t or '?').upper(), _MUTED))[1]}"
                 for t in d["transaction_type"]]
    out = pd.DataFrame({
        "ADDRESS":  d["address"].fillna(""),
        "CITY":     d["city"].fillna(""),
        "TYPE":     d["TYPE"],
        "DATE":     pd.to_datetime(d["sale_date"], errors="coerce"),
        "PRICE":    pd.to_numeric(d["price"], errors="coerce"),
        "%":        pd.to_numeric(d["pct_acquired"], errors="coerce"),
        "IMPLIED":  pd.to_numeric(d["implied_valuation"], errors="coerce"),
        "BUYER":    d["buyer_canonical"].fillna("").where(
                        d["buyer_canonical"].fillna("") != "", d["buyer"].fillna("—")),
        "SELLER":   d["seller"].fillna("—"),
        "ASSET":    d["property_type"].fillna("—"),
        "SF":       pd.to_numeric(d["building_sf"], errors="coerce"),
        "UNITS":    pd.to_numeric(d["unit_count"], errors="coerce"),
        "$/SF":     pd.to_numeric(d["price_per_sf"], errors="coerce"),
        "$/UNIT":   pd.to_numeric(d["price_per_unit"], errors="coerce"),
        "BOOK/PG":  (d["deed_book"].fillna("") + "/" + d["deed_page"].fillna("")
                     ).str.strip("/").replace("", "—"),
    })
    return out


def render(projects: pd.DataFrame | None = None):
    df = load_transactions()
    if df.empty:
        st.info("No transactions loaded yet.")
        return

    # ── Coverage, stated before any total is shown ──────────────────
    n_asset = (df["transaction_type"] == "asset_sale").sum()
    n_part = df["transaction_type"].isin(["partial_interest", "entity_level"]).sum()
    st.markdown(
        f'<div style="font-family:{_MONO};font-size:10px;line-height:1.6;color:{_MUTED};'
        f'border-left:2px solid {_ORANGE};padding:8px 12px;margin:10px 0;background:{_BG2}">'
        f'<b style="color:#e2e8f0">COVERAGE</b> — asset sales are near-complete where a '
        f'registry or assessment feed reaches them. Partial-interest and entity-level deals '
        f'are only as complete as trade-press reporting: a Massachusetts nominee trust can '
        f'move beneficial interests with no deed recorded, so title looks unchanged and no '
        f'deed source can see it. {n_part} such transactions are tracked here, which is a '
        f'floor, not a count. Dollar totals below therefore understate the market by an '
        f'unknown amount.</div>', unsafe_allow_html=True)

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
    assets = ["All"] + sorted(x for x in df["property_type"].dropna().unique() if x)
    asset = c3.selectbox("ASSET CLASS", assets, key="acq_asset")
    years = ["All"] + sorted({str(y)[:4] for y in df["sale_date"].dropna()}, reverse=True)
    year = c4.selectbox("YEAR", years, key="acq_year")

    c5, c6, c7 = st.columns([1, 1, 2])
    lo = c5.number_input("MIN PRICE ($M)", value=0.0, step=1.0, key="acq_lo")
    hi = c6.number_input("MAX PRICE ($M)", value=0.0, step=1.0, key="acq_hi",
                         help="0 means no ceiling.")
    q = c7.text_input("SEARCH", "", key="acq_q", placeholder="address, buyer or seller…")

    f = df.copy()
    if city != "All":
        f = f[f["city"] == city]
    if ttype != "All":
        f = f[f["transaction_type"] == ttype]
    if asset != "All":
        f = f[f["property_type"] == asset]
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
        f'<p style="font-family:{_MONO};font-size:10px;color:{_MUTED};margin:4px 0 8px">'
        f'<span style="color:#e2e8f0;font-weight:700">{len(f)}</span> TRANSACTIONS'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;<span style="color:#e2e8f0;font-weight:700">'
        f'${vol/1e9:.2f}B</span> PAID'
        + "".join(f'&nbsp;&nbsp;·&nbsp;&nbsp;<span style="color:{c}">{m}</span>&nbsp;{lbl}'
                  for m, lbl, c in TYPE_MARK.values())
        + '</p>', unsafe_allow_html=True)

    # ── Table ───────────────────────────────────────────────────────
    _section("TRANSACTIONS")
    disp = _build_display(f)
    w = _widths(disp)
    st.dataframe(
        disp, use_container_width=True, hide_index=True, height=420,
        column_config={
            "ADDRESS": st.column_config.TextColumn(width=w["ADDRESS"], pinned=True),
            "CITY":    st.column_config.TextColumn(width=w["CITY"]),
            "TYPE":    st.column_config.TextColumn(
                width=w["TYPE"],
                help="● ASSET whole property · ◑ STAKE a percentage of the owning "
                     "entity · ◆ ENTITY the owning entity itself · ▲ DISTRESS "
                     "foreclosure or deed in lieu, kept rather than dropped."),
            # Real dates and real numbers, so the headers sort chronologically
            # and by magnitude rather than as text.
            "DATE":    st.column_config.DateColumn(width=w["DATE"], format="YYYY-MM-DD"),
            "PRICE":   st.column_config.NumberColumn(
                width=w["PRICE"], format="$%,d",
                help="What was actually PAID. On a stake this is the stake price, "
                     "never the whole-asset value."),
            "%":       st.column_config.NumberColumn(
                width=w["%"], format="%.0f%%", help="Percentage acquired, on a stake."),
            "IMPLIED": st.column_config.NumberColumn(
                width=w["IMPLIED"], format="$%,d",
                help="Implied WHOLE-ASSET valuation where a source states one. "
                     "Derived arithmetic, not a price paid, and excluded from "
                     "every dollar total."),
            "BUYER":   st.column_config.TextColumn(width=w["BUYER"]),
            "SELLER":  st.column_config.TextColumn(width=w["SELLER"]),
            "ASSET":   st.column_config.TextColumn(width=w["ASSET"]),
            "SF":      st.column_config.NumberColumn(width=w["SF"], format="%,d"),
            "UNITS":   st.column_config.NumberColumn(width=w["UNITS"], format="%d"),
            "$/SF":    st.column_config.NumberColumn(width=w["$/SF"], format="$%,d"),
            "$/UNIT":  st.column_config.NumberColumn(width=w["$/UNIT"], format="$%,d"),
            "BOOK/PG": st.column_config.TextColumn(width=w["BOOK/PG"]),
        })

    # ── Summary ─────────────────────────────────────────────────────
    _section("MOST ACTIVE BUYERS")
    st.caption("By dollars actually paid. A stake contributes its stake price, "
               "not the implied value of the building it sits in.")
    who = f["buyer_canonical"].fillna("").where(
        f["buyer_canonical"].fillna("") != "", f["buyer"].fillna(""))
    top = (f.assign(_who=who).groupby("_who")
             .agg(deals=("id", "count"), paid=("price", "sum"))
             .sort_values("paid", ascending=False).head(15).reset_index())
    top = top[top["_who"] != ""]
    st.dataframe(
        top.rename(columns={"_who": "BUYER", "deals": "DEALS", "paid": "PAID"}),
        use_container_width=True, hide_index=True, height=260,
        column_config={"PAID": st.column_config.NumberColumn(format="$%,d"),
                       "DEALS": st.column_config.NumberColumn(format="%d")})

    a, b = st.columns(2)
    with a:
        _section("VOLUME BY QUARTER")
        qtr = f.dropna(subset=["sale_date"]).copy()
        if not qtr.empty:
            qtr["Q"] = pd.PeriodIndex(pd.to_datetime(qtr["sale_date"]), freq="Q").astype(str)
            st.bar_chart(qtr.groupby("Q")["price"].sum(), height=200)
    with b:
        _section("MEDIAN $/SF BY ASSET CLASS")
        pps = f.dropna(subset=["price_per_sf"])
        if not pps.empty:
            st.bar_chart(pps.groupby("property_type")["price_per_sf"].median()
                            .sort_values(ascending=False).head(12), height=200)
