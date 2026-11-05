import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import math
import json
import pandas as pd
import streamlit as st

from app.data import (
    load_filings, load_cambridge_permits, STAGE_COLORS, review_scale_vocab,
    RESOLUTION_METHODS, resolution_method, shows_provenance_badge, METHOD_ORDER,
    DEVELOPER_CONFIDENCE, developer_confidence, SF_SOURCES, ARCHITECT_SOURCES,
    load_field_citations, RI_SF_NOTE, RI_SF_NOTE_TITLE, UNITS_CONFIDENCE,
    city_options, keep_city_selectable, uses_lifecycle_bar,
    FIELD_TIERS, load_field_tiers, mark_team_value, format_delivery_date,
)
from app.detail_bits import (title_case, programme_chips, summary_line,
                             confidence_dot, source_label, addresses,
                             money_millions, clean, permit_rows, fmt_date,
                             PERMIT_LOOKUP, property_lookup, is_data_endpoint)
from scraper.normalize_developer import is_real_company

# One sentence, reused by all three team columns, so the reader learns the
# marks once rather than three times in three wordings.
_TIER_HELP = (
    "▣ = confirmed in a primary document with the passage quoted, "
    "◉ = from a permit or licence registry, "
    "◈ = web, two independent sources, "
    "◇ = web, a single source, "
    "· = carried forward from earlier work and never checked against a "
    "document. — = no source names one."
)

_NOT_SPECIFIED = "Not specified"


def _sort_key(name: str) -> str:
    """Alphabetise the way the developer list does: a leading 'The' does not count."""
    n = str(name)
    return n[4:].lower() if n.lower().startswith("the ") else n.lower()


def _team_options(df, column: str) -> list[str]:
    """Distinct names present in a project-team column, alphabetised.

    Built from the data rather than a fixed vocabulary, so the list is exactly
    what is selectable and never offers a name that would return nothing.
    """
    seen = {
        str(v).strip() for v in df[column].dropna().unique()
        if str(v).strip() and str(v).strip() != "not_yet_selected"
    }
    return sorted(seen, key=_sort_key)


_BG     = "#0d0f12"
_BG2    = "#141720"
_BORDER = "#1E2530"
_ORANGE = "#F5821E"
_MUTED  = "#8A9BB0"
_UI     = "'Inter', -apple-system, 'Segoe UI', Roboto, sans-serif"
STATUS_COLORS = {
    "Under Review":       _ORANGE,
    "Board Approved":     "#22c55e",
    "Letter of Intent":   "#475569",
    "Under Construction": "#ef4444",
}
STATUS_SHORT = {
    "Under Review":       "REVIEW",
    "Board Approved":     "APPROVED",
    "Letter of Intent":   "LOI",
    "Under Construction": "CONST.",
}
STATUS_DOT = {
    "Under Review":       "◈",
    "Board Approved":     "●",
    "Letter of Intent":   "○",
    "Under Construction": "◆",
}
# "Filed", not "LOI": a letter of intent is a private document between a
# developer and an agency and never appears in a public record. What the
# pipeline actually observes at this stage is a filing.
LIFECYCLE_STAGES = ["FILED", "UNDER REVIEW", "BOARD APPROVED",
                    "UNDER CONST.", "COMPLETE"]
LIFECYCLE_IDX = {
    "Letter of Intent":   0,
    "Under Review":       1,
    "Board Approved":     2,
    "Under Construction": 3,
}

_BAD_DEVS = {"Unknown - review needed", "Unknown", "UNKNOWN",
             "Zoning Petitions for Text Amendments", ""}

# What an empty cell says, matching the detail panel word for word.
NOT_FOUND = "Not found"
_NOT_FOUND_CSS = "color:#4b5563"


def _style_blanks(display: pd.DataFrame):
    """A Styler that writes "Not found" into empty cells and mutes them.

    A Styler rather than string columns, because the three numeric columns have
    to STAY numeric: the screener sorts SF, UNITS and HEIGHT by magnitude, and
    a column of formatted strings sorts "9,925" above "1,234,567". `na_rep`
    puts the words in without touching the underlying values, so the sort is
    unaffected.

    Streamlit gives column_config precedence over a Styler for text and number
    FORMATTING, so the three numeric columns deliberately carry no `format=` in
    their config -- their comma grouping is applied here instead, alongside the
    na_rep that config cannot express.
    """
    def _is_blank(v):
        if v is None or v is pd.NaT:
            return True
        if isinstance(v, float) and v != v:
            return True
        return str(v).strip() in ("", "None", "nan", NOT_FOUND)

    return (display.style
            .format({"SF": "{:,.0f}", "UNITS": "{:,.0f}", "HEIGHT": "{:,.0f} ft"},
                    na_rep=NOT_FOUND)
            .map(lambda v: _NOT_FOUND_CSS if _is_blank(v) else ""))


def _section(label: str):
    st.markdown(
        f'<p style="font-family:{_UI};font-size:9px;font-weight:700;'
        f'letter-spacing:0.18em;color:{_MUTED};text-transform:uppercase;'
        f'margin:16px 0 8px 0">{label}</p>',
        unsafe_allow_html=True,
    )


# ── grouped city dropdown ────────────────────────────────────────────────
# Streamlit's selectbox has no option groups and no disabled options, so the
# grouping is built into the option list itself: a header entry per market,
# and the cities under it indented. A header is not a valid choice, and an
# on_change callback puts the previous city back if one is picked. The
# callback is the reason this works -- session_state cannot be written after
# a widget is instantiated, but it can be written from that widget's own
# callback, which runs before the rerender.
#
# The groups come from the `market` field on every row, never a hardcoded
# list, so a city added to a new market appears under its own header without
# anyone editing this file.
_CITY_HEADER = "── %s ──"
_CITY_INDENT = "  "          # figure space: aligns under the header


def _is_city_header(v) -> bool:
    return isinstance(v, str) and v.startswith("──")


def _city_options(scope):
    """(options, display -> city). Markets alphabetical, cities within them."""
    by_market = {}
    for city, market in zip(scope["city"], scope.get("market", "")):
        if not city:
            continue
        by_market.setdefault(market or "Other", set()).add(city)
    opts, lookup = ["All"], {"All": "All"}
    for market in sorted(by_market):
        head = _CITY_HEADER % market.upper()
        opts.append(head)
        lookup[head] = None                      # not selectable
        for c in sorted(by_market[market]):
            disp = _CITY_INDENT + c
            opts.append(disp)
            lookup[disp] = c
    return opts, lookup


def _keep_city_selectable():
    """Reject a header pick and restore the last real selection."""
    v = st.session_state.get("tbl_city")
    if _is_city_header(v):
        st.session_state["tbl_city"] = st.session_state.get("_tbl_city_prev", "All")
    else:
        st.session_state["_tbl_city_prev"] = v


def _dev_display(row) -> str:
    canonical = str(row["developer_canonical"] or "").strip()
    if canonical and canonical not in _BAD_DEVS and is_real_company(canonical):
        return canonical
    raw = str(row["developer"] or "").strip()
    return raw if raw and raw not in _BAD_DEVS else "—"


# ── Column sizing ─────────────────────────────────────────────────────
# The screener is drawn into a canvas, so nothing in it reflows: a column is
# exactly as wide as it is told to be, and any value wider than that is
# ellipsised away. So every width is MEASURED off the data rather than
# chosen -- each column is sized to the longest string that will actually
# appear in it, its header included.
#
# The measurement is arithmetic, and it USED TO BE EXACT: the theme font was
# monospace and a monospace face advances the same 0.6em for every character.
# The theme is now Inter, which is proportional -- lowercase runs nearer 0.5em,
# digits and capitals nearer 0.6em -- so the same arithmetic is an UPPER BOUND
# rather than an identity. The per-character figure is deliberately held at
# 0.6em, the wide end of Inter's range and the end this table actually lives at,
# since its columns are mostly capitals, digits and currency.
#
# The asymmetry is the point: a column with air in it costs nothing, while a
# column a few pixels too narrow ellipsises its value and the reader never
# learns what was cut. That was the original bug here, and over-estimating is
# how it stays fixed.
#
# The grid draws cells at fontSizes.sm = 14px with 8px of padding either side,
# and the header row additionally carries the sort/menu icon, which is why a
# header costs more than the same text in a cell -- NEIGHBORHOOD and CIVIL
# ENGINEER were clipped headers over columns whose values fitted.
_CHAR_PX = 14.0 * 0.6
_CELL_PAD = 16          # cellHorizontalPadding, both sides
_HEADER_ICON = 18       # headerIconSize, 1.125rem
_MIN_COL_PX = 44
# The formula lands a long value within a pixel of its column edge, which is
# too close to trust: the grid rounds its own text measurement, and the 8px
# padding read out of the bundle is one theme change away from being 10.
# WIDENED FROM 8 WITH THE MOVE TO A PROPORTIONAL FACE, which turned the
# character measurement from exact into approximate.
_SLACK_PX = 14

# Fifteen columns at their full measured width come to 4,270px against the
# full result set, which fits no window. Both ways of forcing a fit lose
# data -- shrink the columns and the values ellipsise mid-word, which is the
# bug this replaced; drop columns and the reader loses them outright. So the
# overflow is not fought: the grid keeps every column at its measured width
# and scrolls sideways, PROJECT is pinned so the row identity stays on
# screen, and a line under the table says that is what is happening. This
# figure is only the threshold for showing that line -- it never sizes a
# column -- and corresponds to a 1664px window in Streamlit's wide layout.
_LAYOUT_BUDGET_PX = 1600

# The provenance marks (▣ ◈ ● ✲ ≠ ⋯ —) are not in the UI face either and are
# drawn from a fallback at roughly one em, so they cost more than a character.
_WIDE_CHAR = 1.6


def _text_px(text: str) -> float:
    return _CHAR_PX * sum(_WIDE_CHAR if ord(c) >= 0x2000 else 1.0 for c in str(text))


def _fmt_int(v, suffix: str = "", comma: bool = False) -> str:
    """Render a number the way its column config will render it."""
    if v is None or pd.isna(v):
        return ""
    return (f"{int(v):,}" if comma else f"{int(v)}") + suffix


# How each column's values reach the canvas. Anything absent is drawn as-is.
def _fmt_date_cell(v) -> str:
    """A DateColumn draws its value in the column's format, so that is what the
    sizer has to measure -- not the raw timestamp."""
    return "" if v is None or v != v else str(v)[:10]


_CELL_TEXT = {
    "SF":        lambda v: _fmt_int(v, comma=True),
    "UNITS":     lambda v: _fmt_int(v),
    "HEIGHT":    lambda v: _fmt_int(v, " ft"),
    "DELIVERED": _fmt_date_cell,
    "TARGET":    _fmt_date_cell,
}


def _column_widths(display: pd.DataFrame) -> dict[str, int]:
    """Size every column to its own longest rendered string, header included."""
    widths = {}
    for col in display.columns:
        render_cell = _CELL_TEXT.get(col, str)
        # A null measures as "nan" but RENDERS as "Not found", so it is
        # measured as what the reader actually sees.
        def _shown(v, _r=render_cell):
            if v is None or v is pd.NaT or (isinstance(v, float) and v != v):
                return NOT_FOUND
            return _r(v)
        longest = max((_text_px(_shown(v)) for v in display[col]), default=0.0)
        header = _text_px(col) + _HEADER_ICON
        widths[col] = max(_MIN_COL_PX, math.ceil(max(longest, header) + _CELL_PAD + _SLACK_PX))
    return widths


def _build_display(filtered: pd.DataFrame) -> pd.DataFrame:
    """The screener frame exactly as the grid draws it.

    Split out of render() so the sizer above measures the same strings the
    reader sees -- the marks, the em dashes and the formatted numbers, not the
    raw database values.
    """

    display = filtered[[
        "name", "developer_canonical", "developer", "architect",
        "civil_engineer", "contractor", "neighborhood", "city",
        "asset_class", "status", "stage", "total_gsf", "residential_units",
        "building_height_ft", "delivered_date", "delivered_precision",
        "target_date", "target_precision",
    ]].copy()

    display["developer_canonical"] = display.apply(_dev_display, axis=1)

    # Every developer name carries its confidence wherever it appears. This
    # used to mark only the INFERRED names, which left confirmed and
    # document-only looking identical -- and document_only is the majority of
    # the Rhode Island data, so an unmarked name read as verified.
    display["_conf"] = filtered["developer_confidence"]
    display["developer_canonical"] = [
        f'{DEVELOPER_CONFIDENCE[c]["mark"]} {name}'
        if c in DEVELOPER_CONFIDENCE and name != "—" else name
        for name, c in zip(display["developer_canonical"], display["_conf"])
    ]
    display.drop(columns=["developer", "_conf"], inplace=True)

    # The three project-team fields sit beside the developer and are marked the
    # same way, for the same reason: most of these names come from a drawing
    # title block, a permit record or a web article rather than from the
    # planning filing, and an unmarked name would read as though the filing
    # itself stated it.
    #
    # The mark comes from field_provenance rather than architect_source. That
    # column only ever described the architect and only recorded the channel;
    # the provenance table covers all three fields and records how strongly
    # each value is evidenced, which is the thing a reader needs to weigh.
    #
    # A blank contractor is deliberately two different things. On a project
    # that has not broken ground there is nobody to name yet, and rendering
    # that as an em dash beside a project we searched and came up empty would
    # be the table lying about its own coverage.
    _tiers = load_field_tiers()
    _ids = list(filtered["id"])
    for _col, _field in (("architect", "architect"),
                         ("civil_engineer", "civil_engineer"),
                         ("contractor", "general_contractor")):
        display[_col] = [
            mark_team_value(v, _tiers.get((int(pid), _field)))
            for pid, v in zip(_ids, display[_col].fillna(""))
        ]

    def _status_fmt(row):
        if not row["status"]:
            return "—"
        if row["status"] in STATUS_DOT:
            return f"{STATUS_DOT[row['status']]} {STATUS_SHORT[row['status']]}"
        # Cambridge (or any city outside the Boston vocab): dot colored by
        # normalized stage, label is the native status text.
        return f"● {row['status']}"

    display["status_fmt"] = display.apply(_status_fmt, axis=1)

    # SF carries its provenance, for the same reason developer names do: a
    # web-sourced figure and a filing-stated one must not read alike.
    display["_sf_src"] = filtered["total_gsf_source"]
    display["total_gsf"] = pd.to_numeric(display["total_gsf"], errors="coerce")
    display["residential_units"] = pd.to_numeric(display["residential_units"], errors="coerce")
    display["building_height_ft"] = pd.to_numeric(display["building_height_ft"], errors="coerce")

    # SF stays a NUMBER so the column sorts numerically. Formatting a figure
    # into a string and handing that to a TextColumn made 99,925 sort above
    # 1,234,567, because as text "9" beats "1" -- every Boston project over
    # 100,000 sq ft was ranking below far smaller ones. Formatting is display
    # only, done by the column config. Provenance moves to its own narrow
    # column rather than being welded into the value, which is what forced the
    # value to be text in the first place.
    # DELIVERED and TARGET are two columns, not one field with a marker.
    # A marker does not survive CSV export -- a forecast and an actual would
    # land in the same spreadsheet column and be counted together, which is
    # the one error this table must not make. So an unfinished project has a
    # TARGET and no DELIVERED, a finished one has a DELIVERED and no TARGET,
    # and no row ever carries both.
    #
    # Both columns hand the grid the REAL DATE, not a rendered label, so that
    # clicking the header sorts chronologically in both directions. Handing it
    # "Q2 2026" and "2025" instead made the header sort compare text, which is
    # not an approximation of date order but unrelated to it: measured over the
    # full 727 rows it put 548 TARGET pairs backwards, because every bare year
    # sorts ahead of every "Q..." value whatever the date, and DELIVERED
    # sorted by day-of-month because "25 Feb 2025" begins with 25.
    #
    # The cost is that a date column formats uniformly, so a quarter- or
    # year-precision value shows as the first day of its period. The precision
    # itself is not lost -- it is stored, and the detail panel prints the value
    # at its true precision beside the vintage.
    display["delivered_fmt"] = pd.to_datetime(display["delivered_date"], errors="coerce")
    display["target_fmt"] = pd.to_datetime(display["target_date"], errors="coerce")
    # An empty text cell renders as a blank, which reads as "nothing here" when
    # what it means is "no source states one" -- the same distinction the detail
    # panel draws. Said in words, and muted by the Styler below so it never
    # reads as a value.
    for _blank in ("neighborhood", "asset_class"):
        display[_blank] = [
            NOT_FOUND if (v is None or str(v).strip() in ("", "None", "nan")) else v
            for v in display[_blank]
        ]

    display = display[[
        "name", "developer_canonical", "architect", "civil_engineer", "contractor",
        "neighborhood", "city",
        "asset_class", "status_fmt", "total_gsf",
        "residential_units", "building_height_ft", "delivered_fmt", "target_fmt",
    ]]
    display.columns = [
        "PROJECT", "DEVELOPER", "ARCHITECT", "CIVIL ENGINEER", "CONTRACTOR",
        "NEIGHBORHOOD", "CITY",
        "TYPE", "STATUS", "SF",
        "UNITS", "HEIGHT", "DELIVERED", "TARGET",
    ]
    return display


def render(df: pd.DataFrame):
    # ── Filter toolbar ─────────────────────────────────────────────
    _section("FILTER")

    # SHOW comes first because it decides what the table is FOR. A delivered
    # building and a withdrawn application are worth keeping and worth being
    # able to look at, but they are not pipeline, and listing them by default
    # put 70 finished buildings and 29 dead applications in front of the
    # reader as if they were live. Pipeline is the default; the others are one
    # click away rather than mixed in.
    # THREE ROWS OF FOUR, not one row of nine.
    #
    # Nine controls across one row left each about a tenth of the width, which
    # is narrower than the values they hold: SHOW rendered "Withdra", MARKET
    # rendered "Massachus" and "Rhode Isl". Equal columns across every row also
    # make the rows line up, which one row of nine and two ragged half-width
    # rows underneath never did.
    #
    # Grouped by what the reader is asking: where is it, what is it, who is
    # building it. Free-text search sits on its own line under the grid.
    _css = """
    <style>
      /* An open menu sizes to its longest option instead of to the field. */
      div[data-baseweb="popover"] ul[role="listbox"] { min-width: max-content !important; }
      div[data-baseweb="popover"] li { white-space: nowrap !important; }
      /* The closed field shows its whole value. Most fit on one line at this
         width; the one outlier is Cambridge's "Approved PUD/Master Plan
         Development Remaining" at 442px against 354px of field, and widening
         every column by a quarter for one status would be the wrong trade, so
         a value that does not fit wraps and the field grows instead. */
      div[data-baseweb="select"] div[title] {
        white-space: normal;
        overflow: visible;
        text-overflow: clip;
        line-height: 1.25;
      }
      div[data-testid="stSelectbox"] div[data-baseweb="select"] > div {
        height: auto;
        min-height: 38px;
      }
    </style>"""
    st.markdown(_css, unsafe_allow_html=True)

    r1a, r1b, r1c, r1d = st.columns(4)          # where
    r2a, r2b, r2c, r2d = st.columns(4)          # what
    fshow, fcm, fc0, fc1 = r1a, r1b, r1c, r1d
    fc2, fcft, fc3, fc4 = r2a, r2b, r2c, r2d
    SHOW_OPTS = ["Pipeline", "Delivered", "Withdrawn / denied", "All"]
    show = fshow.selectbox(
        "SHOW", SHOW_OPTS, key="tbl_show",
        help="Pipeline excludes buildings established as complete and "
             "applications that were withdrawn or denied. They stay in the "
             "tracker and are one selection away.")
    _delivered = df["stage"] == "Complete" if "stage" in df.columns else pd.Series(False, index=df.index)
    _dead = (df["project_status_filing"].isin(["Withdrawn", "Denied"])
             if "project_status_filing" in df.columns else pd.Series(False, index=df.index))
    if show == "Pipeline":
        df = df[~_delivered & ~_dead]
    elif show == "Delivered":
        df = df[_delivered]
    elif show == "Withdrawn / denied":
        df = df[_dead]

    # Market groups the cities so the two states can be compared without
    # selecting five municipalities one at a time. The city list is scoped to
    # the chosen market rather than listing all eleven regardless.
    markets = ["All"] + sorted([m for m in df["market"].unique() if m])
    market = fcm.selectbox("MARKET", markets, key="tbl_market")
    city_scope = df if market == "All" else df[df["market"] == market]

    # Cities grouped under their state. Built from city_scope, so choosing a
    # MARKET narrows the list to that state's cities and drops the other
    # header entirely -- the two filters agree instead of contradicting.
    city_opts, city_lookup = city_options(city_scope)
    if st.session_state.get("tbl_city") not in city_opts:
        st.session_state["tbl_city"] = "All"      # market change orphaned it
    city_disp = fc0.selectbox("CITY", city_opts, key="tbl_city",
                              on_change=keep_city_selectable("tbl_city"))
    city = city_lookup.get(city_disp) or "All"

    # The square-footage column is 15% filled for Rhode Island, and without the
    # reason on the page that reads as a broken tracker rather than as a fact
    # about how the market files. Shown whenever the view is scoped to Rhode
    # Island, collapsed so it explains without interrupting.
    _ri_cities = {"Providence", "Warwick", "Cranston", "Pawtucket", "Newport"}
    _showing_ri = (market == "Rhode Island") or (city in _ri_cities)

    # Status vocab is city-specific (Boston's 4 values vs Cambridge's 7 don't
    # overlap), so scope the status options to whatever city is selected --
    # otherwise "All" would show all 11 values mixed together.
    status_scope = city_scope if city == "All" else city_scope[city_scope["city"] == city]
    neighborhoods = ["All"] + sorted([n for n in status_scope["neighborhood"].unique() if n])
    nbhd = fc1.selectbox("NEIGHBORHOOD", neighborhoods, key="tbl_nbhd")

    statuses = ["All"] + sorted([s for s in status_scope["status"].unique() if s])
    status = fc2.selectbox("STATUS", statuses, key="tbl_status")

    # FILING TYPE is its own axis. It was previously poured into STATUS, so the
    # status dropdown offered "Lot Merger" and "Design Waiver" as if they were
    # pipeline stages. They are the action a board was asked to take, which is
    # a genuinely useful filter -- just not the same question.
    ft_opts = ["All"] + sorted([f for f in status_scope.get(
        "filing_type", pd.Series(dtype=str)).unique() if f])
    filing_type = (fcft.selectbox("FILING TYPE", ft_opts, key="tbl_filing_type")
                   if len(ft_opts) > 1 else "All")

    # Scale vocabulary is market-specific (Article 80's two tiers vs. RIGL's
    # three), so scope the options to the selected city via the registry rather
    # than hardcoding one market's values.
    scale_opts = review_scale_vocab([city] if city != "All" else df["city"].unique())
    scale = fc3.selectbox("SCALE", ["All"] + scale_opts, key="tbl_scale")

    classes = ["All"] + sorted([a for a in df["asset_class"].unique() if a])
    asset = fc4.selectbox("ASSET CLASS", classes, key="tbl_asset")

    # Row three: who is building it. Same four equal columns as the rows
    # above, so the grid lines up instead of stepping between a short field
    # and a full-width one.
    fd1, fd2, fd3, fd4 = st.columns(4)
    all_devs = sorted(
        {d for d in df["developer_canonical"].unique() if is_real_company(d)},
        key=lambda x: x.lstrip("Tt").lower() if x.lower().startswith("the ") else x.lower()
    )
    dev_search = fd1.text_input("DEVELOPER SEARCH", "", key="tbl_dev_search",
                                placeholder="e.g. Marcus Partners")
    matching_devs = (
        [d for d in all_devs if dev_search.lower() in d.lower()]
        if dev_search else all_devs
    )
    developer = fd2.selectbox("DEVELOPER", ["All"] + matching_devs, key="tbl_developer")

    # The other two members of the project team, filtered the same way the
    # developer is: a plain dropdown over the distinct names actually present,
    # alphabetised the same way, defaulting to All. Both carry a "Not
    # specified" option, because "which projects still have no civil engineer"
    # is the question this table gets asked most often now that the column
    # exists and is only two-fifths full.
    #
    # These sit where DEVELOPER SOURCE and DEVELOPER CONFIDENCE were. Both of
    # those described how one field was established, which the developer name
    # already shows inline through its confidence mark; the team columns had
    # no way to be filtered at all.
    architect_f = fd3.selectbox(
        "ARCHITECT", ["All", _NOT_SPECIFIED] + _team_options(df, "architect"),
        key="tbl_architect",
        help="Architecture practice, or the individual where a record names "
             "only a person. 'Not specified' selects projects where no source "
             "names one.",
    )
    civil_f = fd4.selectbox(
        "CIVIL ENGINEER", ["All", _NOT_SPECIFIED] + _team_options(df, "civil_engineer"),
        key="tbl_civil",
        help="Civil engineer only -- a surveyor, traffic engineer or landscape "
             "architect is not one and is not listed here. 'Not specified' "
             "selects projects where no source names one.",
    )

    # Free-text search last and full width. It is the one control whose useful
    # length is unbounded, so it gets the row rather than a ninth of one.
    search = st.text_input("SEARCH", "", key="tbl_search",
                           placeholder="project name or address…")

    # The note sits under the whole filter block rather than between two rows
    # of it, so the grid stays unbroken.
    if _showing_ri:
        with st.expander(RI_SF_NOTE_TITLE, expanded=False):
            st.markdown(RI_SF_NOTE)

    # Apply filters
    filtered = df.copy()
    if market != "All":
        filtered = filtered[filtered["market"] == market]
    if city != "All":
        filtered = filtered[filtered["city"] == city]
    if nbhd != "All":
        filtered = filtered[filtered["neighborhood"] == nbhd]
    if status != "All":
        filtered = filtered[filtered["status"] == status]
    if filing_type != "All":
        filtered = filtered[filtered["filing_type"] == filing_type]
    if scale != "All":
        filtered = filtered[filtered["review_scale"] == scale]
    if asset != "All":
        filtered = filtered[filtered["asset_class"] == asset]
    if developer != "All":
        filtered = filtered[filtered["developer_canonical"] == developer]
    # Both team filters narrow the same frame every other filter has already
    # narrowed, so they compose with the rest and with each other. A blank is
    # matched on the stripped value, because these columns hold "" and None
    # interchangeably depending on which pass last wrote them.
    for _sel, _col in ((architect_f, "architect"), (civil_f, "civil_engineer")):
        if _sel == "All":
            continue
        _vals = filtered[_col].fillna("").astype(str).str.strip()
        filtered = (filtered[_vals == ""] if _sel == _NOT_SPECIFIED
                    else filtered[_vals == _sel])
    if search:
        q = search.lower()
        mask = (
            filtered["name"].str.lower().str.contains(q, na=False) |
            filtered["address"].str.lower().str.contains(q, na=False)
        )
        # A case filed on several parcels names them all, and the ingest kept
        # only the first. Searching 1077 Westminster Street returned nothing
        # for a project that is in the tracker as 311 Knight Street.
        if "alt_addresses" in filtered.columns:
            mask = mask | filtered["alt_addresses"].str.lower().str.contains(q, na=False)
        filtered = filtered[mask]

    # ── Default order: biggest first ───────────────────────────────
    # Sorted HERE, on `filtered`, rather than on the display frame, because the
    # detail panel resolves a click as filtered.iloc[idx] -- sorting only the
    # display frame would open the wrong project. Sorting before the count and
    # the export means the CSV comes out in the same order the screener shows.
    #
    # na_position="last" is the point: a project with no stated size is not a
    # small project, and floating it to the top of a table sorted by size would
    # say it was. kind="stable" keeps the previous ordering as the tie-break,
    # so rows of equal size do not shuffle between reruns.
    if "total_gsf" in filtered.columns:
        filtered = filtered.sort_values(
            "total_gsf", ascending=False, na_position="last", kind="stable")

    cnt_col, exp_col = st.columns([5, 1])
    cnt_col.markdown(
        f'<p style="font-family:{_UI};font-size:10px;color:{_MUTED};margin:4px 0 8px">'
        f'<span style="color:#e2e8f0;font-weight:700">{len(filtered)}</span> PROJECTS'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;{len(df)} TOTAL'
        + "".join(
            f'&nbsp;&nbsp;·&nbsp;&nbsp;<span style="color:{v["color"]}">{v["mark"]}</span>'
            f'&nbsp;{v["label"].upper()}'
            for k, v in DEVELOPER_CONFIDENCE.items()
        )
        + '</p>',
        unsafe_allow_html=True,
    )
    csv = filtered.to_csv(index=False).encode()
    exp_col.download_button("↓ EXPORT CSV", csv, "boston_cre_pipeline.csv", "text/csv")

    # ── Table ─────────────────────────────────────────────────────
    _section("SCREENER")

    display = _build_display(filtered)

    # Widths measured off the frame above, not chosen: each column is as wide
    # as its own longest rendered string, header included. Recomputed on every
    # filter, so narrowing the view narrows the table with it.
    widths = _column_widths(display)

    selection = st.dataframe(
        _style_blanks(display),
        use_container_width=True,
        hide_index=True,
        height=400,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            # PROJECT is pinned: it is the column that says which row you are
            # looking at, and it has to stay on screen while the rest scrolls
            # under it.
            "PROJECT": st.column_config.TextColumn(width=widths["PROJECT"], pinned=True),
            "DEVELOPER": st.column_config.TextColumn(width=widths["DEVELOPER"]),
            # Numeric so it sorts by magnitude; the comma is display only.
            # Blanks sort to the end rather than being coerced to zero, which
            # would rank an unknown size alongside a genuine nothing.
            # The arrow says the table opens sorted on this column. Streamlit
            # exposes no way to set a grid's sort indicator, and no way to
            # learn that the reader has since clicked another header -- so the
            # arrow marks the STARTING order, which is what it is for.
            "SF":     st.column_config.NumberColumn(
                "SF ↓", width=widths["SF"],
                help="Gross square feet, as stated by the source. The table "
                     "opens sorted by this column, largest first, with "
                     "unknown sizes last — an unstated size is not a small "
                     "project. Click any header to re-sort."),
            "ARCHITECT": st.column_config.TextColumn(
                width=widths["ARCHITECT"],
                help="Architecture practice where one is named, otherwise the "
                     "individual the record names. " + _TIER_HELP),
            "CIVIL ENGINEER": st.column_config.TextColumn(
                width=widths["CIVIL ENGINEER"],
                help="Civil engineer only. A surveyor, traffic engineer, "
                     "structural engineer or landscape architect is not one, "
                     "and is not counted here. " + _TIER_HELP),
            "CONTRACTOR": st.column_config.TextColumn(
                width=widths["CONTRACTOR"],
                help="General contractor or construction manager. "
                     "'⋯ not yet appointed' means construction has not "
                     "started, so no contractor exists yet — that is not the "
                     "same as — , which means we searched and no source names "
                     "one. " + _TIER_HELP),
            "NEIGHBORHOOD": st.column_config.TextColumn(width=widths["NEIGHBORHOOD"]),
            "CITY":   st.column_config.TextColumn(width=widths["CITY"]),
            "TYPE":   st.column_config.TextColumn(width=widths["TYPE"]),
            "STATUS": st.column_config.TextColumn(width=widths["STATUS"]),
            # No format= on either: column_config formatting would override the
            # Styler, taking the "Not found" with it.
            "UNITS":  st.column_config.NumberColumn(width=widths["UNITS"]),
            "HEIGHT": st.column_config.NumberColumn(width=widths["HEIGHT"]),
            "DELIVERED": st.column_config.DateColumn(
                width=widths["DELIVERED"], format="MMM D, YYYY",
                help="When the building was actually finished. Blank means it "
                     "has not delivered, or that it has and no source states "
                     "the date — the detail panel says which. Click the header "
                     "to sort chronologically, again to reverse. Where the "
                     "source gave only a quarter or a year this shows the "
                     "first day of that period; the detail panel prints it at "
                     "its true precision."),
            "TARGET": st.column_config.DateColumn(
                width=widths["TARGET"], format="MMM D, YYYY",
                help="Forecast completion, and only ever a forecast: it goes "
                     "blank the moment a project delivers, so nothing in this "
                     "column can be mistaken for something that happened. "
                     "Shown at the source's precision — Q2 2026 means the "
                     "quarter, not 1 April — this column shows the first day "
                     "of the period and the detail panel prints the true "
                     "precision. Click the header to sort chronologically, "
                     "again to reverse. A forecast is only as good as its "
                     "vintage, so who stated it and when are in the detail "
                     "panel and in the CSV export."),
        },
    )

    # Say so when the table is wider than the window, because a grid that
    # scrolls sideways looks identical to one that ends at the right edge.
    _total_px = sum(widths.values())
    if _total_px > _LAYOUT_BUDGET_PX:
        st.markdown(
            f'<p style="font-family:{_UI};font-size:10px;color:{_MUTED};margin:4px 0 0">'
            f'FULL-WIDTH COLUMNS TOTAL {_total_px:,}PX — WIDER THAN THE WINDOW. '
            f'THE TABLE SCROLLS SIDEWAYS RATHER THAN SHRINKING; PROJECT STAYS PINNED. '
            f'NARROW THE FILTERS TO NARROW THE TABLE.</p>',
            unsafe_allow_html=True,
        )


    # ── Detail panel ──────────────────────────────────────────────
    if selection and selection.selection.rows:
        idx = selection.selection.rows[0]
        _detail_panel(filtered.iloc[idx], df)


def _parcel_detail(p) -> str | None:
    """The parcel the project is keyed on, plus the rest of an assembled site.

    The aliases are shown rather than kept internal because they are the
    answer to "why is this one row and not three" -- a reader looking at a
    tower that occupies four lots needs to see the other three accounted for.
    """
    pid = (p.get("parcel_id") or "").strip()
    if not pid:
        return None
    aliases = [a.strip() for a in (p.get("parcel_id_aliases") or "").split(",") if a.strip()]
    if not aliases:
        return pid
    return (f'{pid}<br><span style="color:{_MUTED};font-size:9px">'
            f'+{len(aliases)} more parcel(s): {", ".join(aliases)}</span>')


def _delivered_detail(p) -> str | None:
    """The actual completion date, with what established it."""
    d = format_delivery_date(p.get("delivered_date"), p.get("delivered_precision"))
    if not d:
        # Complete with no date is a real state and a different one from not
        # complete. Saying so beats a blank that reads like an oversight.
        if (p.get("completion_stage") or "") == "Complete":
            basis = (p.get("completion_basis") or "").replace("_", " ")
            return (f'<span style="color:{_MUTED}">complete, but no source '
                    f'states the date{f" ({basis})" if basis else ""}</span>')
        return None
    basis = (p.get("completion_basis") or "").replace("_", " ")
    return d + (f' <span style="color:{_MUTED}">({basis})</span>' if basis else "")


def _target_detail(p) -> str | None:
    """The forecast, with its vintage. A 2026 completion stated in 2021 and
    one stated last month are different claims and must not read alike."""
    t = format_delivery_date(p.get("target_date"), p.get("target_precision"))
    if not t:
        return None
    bits = []
    who = (p.get("target_stated_by") or "").strip()
    when = p.get("target_stated_on")
    when_s = format_delivery_date(when, "day") if when is not None and when == when else ""
    if when_s:
        bits.append(f"stated {when_s}")
    if who:
        bits.append(f"by {who}")
    vintage = f' <span style="color:{_MUTED}">({", ".join(bits)})</span>' if bits else ""
    # The verbatim phrase, where the parse dropped something -- a range, a
    # season. Without it "Q2 2024" looks like a quote and it is not.
    raw = (p.get("expected_delivery") or "").strip()
    verbatim = (f'<br><span style="color:{_MUTED};font-size:10px">source wording: '
                f'"{raw}"</span>') if raw and raw.lower() != t.lower() else ""
    return t + vintage + verbatim


def _lifecycle_bar(status: str) -> str:
    cur = LIFECYCLE_IDX.get(status, -1)
    items = []
    for i, stage in enumerate(LIFECYCLE_STAGES):
        if i < cur:
            dot = f"background:#22c55e;border:1.5px solid #22c55e"
            lbl_c = "#22c55e"
        elif i == cur:
            dot = f"background:{_ORANGE};border:1.5px solid {_ORANGE}"
            lbl_c = _ORANGE
        else:
            dot = f"background:{_BG};border:1.5px solid {_BORDER}"
            lbl_c = _BORDER

        connector = ""
        if i > 0:
            line_c = "#22c55e" if i <= cur else _BORDER
            connector = (
                f'<div style="flex:1;height:1px;background:{line_c};'
                f'margin-top:5px;min-width:8px"></div>'
            )
        items.append(
            connector +
            f'<div style="display:flex;flex-direction:column;align-items:center;gap:4px">'
            f'<div style="width:10px;height:10px;border-radius:50%;{dot}"></div>'
            f'<div style="font-family:{_UI};font-size:7.5px;font-weight:700;'
            f'letter-spacing:0.1em;color:{lbl_c};text-align:center;white-space:nowrap">{stage}</div>'
            f'</div>'
        )
    return (
        f'<div style="display:flex;align-items:flex-start;gap:0;'
        f'margin:14px 0 18px;padding:12px 16px;'
        f'background:{_BG2};border:1px solid {_BORDER}">'
        + "".join(items) +
        f'</div>'
    )


# Asset classes with no dwellings in them. A blank unit count on a warehouse
# is not a gap in the research -- there is nothing to count -- and showing it
# the same way as a genuinely unknown figure makes 85 correct blanks look like
# 85 holes. Institutional and Other are deliberately NOT here: a dormitory is
# institutional and does have units, so those stay "not published" rather than
# claiming the field does not apply.
NO_DWELLINGS = {"Retail", "Office", "Industrial", "Parking", "Lab"}


def _lender_detail(p) -> str:
    """The construction lender, or which KIND of blank the field is.

    Three blanks, and conflating them is what made lender coverage look like a
    research failure when most of it was not one:

        No construction loan reported   a source documents how the project is
                                        financed -- sponsor equity, presales,
                                        conduit bonds, an owner-occupier
                                        building for itself
        lender not named                a construction loan IS documented and
                                        no acceptable source says who made it
        not found                       nothing was found

    "Reported" is deliberate. No news of a loan is not proof there is no loan.
    """
    if p.get("lender"):
        return p["lender"]
    status = (p.get("lender_status") or "").strip()
    if status == "no_loan_reported":
        return _absent("no construction loan reported")
    if status == "lender_unnamed":
        return _absent("a construction loan is reported; no source names the lender")
    # Nothing known: return None so the card renders its own "Not found",
    # keeping every empty row in the panel identical. The two labels above are
    # findings and deliberately look different from an empty field.
    return None


def _absent(reason: str) -> str:
    """An em dash plus why, for a field with no value.

    `_kv` drops an empty field from the panel entirely, which is right for a
    field that is merely irrelevant and wrong for height and unit count -- the
    two the pipeline is read for. An omitted row cannot be told apart from one
    that was never researched, so these two always print, and print which kind
    of blank they are.
    """
    return (f'<span style="color:{_MUTED}">&mdash; {reason}</span>')


def _kv(label: str, value) -> str:
    if not value or (isinstance(value, float) and value != value):
        return ""
    return (
        f'<div style="margin-bottom:10px">'
        f'<div style="font-family:{_UI};font-size:8.5px;font-weight:700;'
        f'letter-spacing:0.12em;color:{_MUTED};text-transform:uppercase;margin-bottom:3px">{label}</div>'
        f'<div style="font-family:{_UI};font-size:12px;color:#e2e8f0;font-weight:500">{value}</div>'
        f'</div>'
    )



# ── panel building blocks ───────────────────────────────────────────────
# Every tile and every card row renders at the same size whether or not it has
# a value, so two panels side by side have the same shape and a reader learns
# where to look once. A missing value reads "Not found" in muted text and is
# never hidden, because a hidden row cannot be told apart from a field that
# does not apply to this project.
_NOT_FOUND = '<span style="color:#4b5563">Not found</span>'


def _tile(label: str, value) -> str:
    """One of the six key-number tiles. Flexes, so it wraps on a narrow screen."""
    shown = clean(value) or _NOT_FOUND
    return (
        f'<div style="flex:1 1 150px;min-width:118px;background:{_BG2};'
        f'border:1px solid {_BORDER};padding:10px 12px;box-sizing:border-box">'
        f'<div style="font-family:{_UI};font-size:8.5px;font-weight:700;'
        f'letter-spacing:0.12em;color:{_MUTED};text-transform:uppercase;'
        f'margin-bottom:5px;white-space:nowrap">{label}</div>'
        f'<div style="font-family:{_UI};font-size:15px;font-weight:600;'
        f'color:#e2e8f0;line-height:1.25">{shown}</div></div>'
    )


def _card(title: str, rows) -> str:
    """A titled card of label/value rows. Every row renders, empty or not."""
    body = ""
    for label, value in rows:
        # clean() turns a pipeline placeholder -- "not_yet_selected", "TBD",
        # "Unknown - review needed" -- into an empty field, so a value nobody
        # has chosen yet reads the same as one nobody has found.
        shown = clean(value) or _NOT_FOUND
        body += (
            f'<div style="display:flex;gap:10px;padding:7px 0;'
            f'border-bottom:1px solid rgba(30,37,48,0.75)">'
            f'<div style="flex:0 0 128px;font-family:{_UI};font-size:8.5px;'
            f'font-weight:700;letter-spacing:0.12em;color:{_MUTED};'
            f'text-transform:uppercase;padding-top:3px">{label}</div>'
            f'<div style="flex:1;font-family:{_UI};font-size:12.5px;'
            f'color:#e2e8f0;line-height:1.5;word-break:break-word">{shown}</div>'
            f'</div>'
        )
    return (
        f'<div style="background:{_BG2};border:1px solid {_BORDER};'
        f'padding:12px 14px 8px;height:100%;box-sizing:border-box">'
        f'<div style="font-family:{_UI};font-size:9px;font-weight:700;'
        f'letter-spacing:0.16em;color:{_ORANGE};text-transform:uppercase;'
        f'margin-bottom:4px">{title}</div>{body}</div>'
    )


def _badge(text: str, color: str, outlined: bool = False) -> str:
    border = color if outlined else _BORDER
    return (f'<span style="font-family:{_UI};font-size:9px;font-weight:700;'
            f'letter-spacing:0.1em;color:{color};border:1px solid {border};'
            f'padding:3px 8px;text-transform:uppercase;white-space:nowrap">'
            f'{text}</span>')


def _chip(text: str) -> str:
    return (f'<span style="font-family:{_UI};font-size:10px;color:#cbd5e1;'
            f'background:rgba(138,155,176,0.10);border:1px solid {_BORDER};'
            f'padding:4px 10px;border-radius:11px;white-space:nowrap">{text}</span>')


def _num(p, key):
    """An int from a possibly-missing, possibly-NaN field."""
    v = p.get(key)
    try:
        if v is None or v != v:
            return None
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _display_name(p) -> str:
    """The project name, cased for reading.

    A permit-derived row that research has not yet named carries its street
    address as its name, so it gets the same treatment an address gets:
    "2961 BIRD AV" reads "2961 Bird Ave". A real project name is left exactly
    as it was recorded.
    """
    name = str(p.get("name") or "")
    if name and name == str(p.get("address") or ""):
        return title_case(name, address=True)
    return name


def _detail_panel(p: pd.Series, df: pd.DataFrame):
    st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)

    is_cambridge = p.get("city") == "Cambridge"
    status_color = STATUS_COLORS.get(p["status"]) or STAGE_COLORS.get(p.get("stage"), _MUTED)
    status_short = STATUS_SHORT.get(p["status"], p["status"])

    # ── 1. Header: name, badges, one plain-English line ─────────────
    _badges = [_badge(status_short, status_color, outlined=True)]
    if p.get("asset_class"):
        _badges.append(_badge(p["asset_class"], "#cbd5e1"))
    if p.get("neighborhood"):
        _badges.append(_badge(p["neighborhood"], _MUTED))
    if p.get("city"):
        _badges.append(_badge(p["city"], _MUTED))

    st.markdown(
        f'<div style="border-left:3px solid {_ORANGE};padding:12px 16px 12px;'
        f'background:{_BG2};border-top:1px solid {_BORDER};'
        f'border-right:1px solid {_BORDER};border-bottom:1px solid {_BORDER}">'
        f'<div style="font-family:{_UI};font-size:9px;font-weight:700;'
        f'letter-spacing:0.14em;color:{_MUTED};text-transform:uppercase;'
        f'margin-bottom:6px">PROJECT DETAIL</div>'
        f'<div style="font-family:Inter,sans-serif;font-size:1.15rem;'
        f'font-weight:700;color:#ffffff;margin-bottom:9px;line-height:1.3">'
        f'{_display_name(p)}</div>'
        f'<div style="display:flex;gap:7px;align-items:center;flex-wrap:wrap;'
        f'margin-bottom:9px">{"".join(_badges)}</div>'
        # Built from the structured fields, never from the permit text, so it
        # says the same kind of thing on every row in every market.
        f'<div style="font-family:Inter,sans-serif;font-size:12.5px;'
        f'color:#cbd5e1;line-height:1.5">{summary_line(p)}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # A stage derived from an agenda alone means the project was SCHEDULED at
    # that stage, not that the outcome was recorded. Surfacing this is the whole
    # point of the two-field split -- without it, "heard at Final Plan" and
    # "Final Plan approved" would look identical on the detail view.
    if p.get("stage_provisional"):
        heard = p.get("stage_heard") or p.get("stage") or "—"
        st.markdown(
            f'<div style="border:1px solid #f59e0b;background:rgba(245,158,11,0.08);'
            f'padding:10px 16px;margin:10px 0;font-family:{_UI};font-size:11px;'
            f'color:#f59e0b;line-height:1.5">'
            f'⚠ STAGE NOT CONFIRMED — this project was heard at <b>{heard}</b> per the '
            f'meeting agenda, but no minutes recording the outcome are available, so the '
            f'vote result is unknown. The stage shown reflects the furthest stage reached '
            f'on an agenda, not an approval.</div>',
            unsafe_allow_html=True,
        )

    if p.get("conditional_alternative"):
        st.markdown(
            f'<div style="border:1px solid #f59e0b;background:rgba(245,158,11,0.08);'
            f'padding:10px 16px;margin:10px 0;font-family:{_UI};font-size:11px;'
            f'color:#f59e0b;line-height:1.5">'
            f'⚠ COMPETING PLAN — this project shares a special permit base number with '
            f'other current-edition entries under different amendments. It represents one '
            f'of two or more alternative build-outs for the same site; the developer has not '
            f'finalized which will proceed. Excluded from aggregate totals by default.</div>',
            unsafe_allow_html=True,
        )
    if p.get("spans_municipalities"):
        st.markdown(
            f'<div style="border:1px solid {_MUTED};background:rgba(138,155,176,0.06);'
            f'padding:8px 16px;margin:6px 0;font-family:{_UI};font-size:10px;'
            f'color:{_MUTED}">↔ Spans more than one municipality — see description/notes.</div>',
            unsafe_allow_html=True,
        )

    # Lifecycle bar (Boston's Article 80 phase sequence). It is drawn only for
    # a status that is actually a point on that sequence -- Cambridge's
    # permitting tracks and South Florida's evidence-driven stages are not, and
    # drawing their status onto Article 80's five phases would place every one
    # of them at phase zero. Keyed on the vocabulary rather than on the city,
    # so the next market added gets the right panel without another branch.
    if not (uses_lifecycle_bar(p.get("city")) and p["status"] in LIFECYCLE_IDX):
        stage = p.get("stage") or "—"
        stage_color = STAGE_COLORS.get(stage, _MUTED)
        st.markdown(
            f'<div style="margin:12px 0 16px;padding:10px 16px;background:{_BG2};'
            f'border:1px solid {_BORDER};font-family:{_UI};font-size:10px;'
            f'display:flex;align-items:center;gap:10px">'
            f'<span style="color:{_MUTED};letter-spacing:0.1em">STAGE</span>'
            f'<span style="color:{stage_color};font-weight:700;letter-spacing:0.08em">{stage.upper()}</span>'
            f'<span style="color:{_MUTED}">(native status: {p["status"]})</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(_lifecycle_bar(p["status"]), unsafe_allow_html=True)

    # ── 2. Pipeline bar ─────────────────────────────────────────────
    if p["status"] not in LIFECYCLE_IDX:
        stage = p.get("stage") or "—"
        stage_color = STAGE_COLORS.get(stage, _MUTED)
        st.markdown(
            f'<div style="margin:12px 0 14px;padding:10px 16px;background:{_BG2};'
            f'border:1px solid {_BORDER};font-family:{_UI};font-size:10px;'
            f'display:flex;align-items:center;gap:10px;flex-wrap:wrap">'
            f'<span style="color:{_MUTED};letter-spacing:0.1em">STAGE</span>'
            f'<span style="color:{stage_color};font-weight:700;'
            f'letter-spacing:0.08em">{stage.upper()}</span>'
            f'<span style="color:{_MUTED}">(native status: {p["status"]})</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(_lifecycle_bar(p["status"]), unsafe_allow_html=True)

    # ── 3. Key numbers: six tiles, always six ───────────────────────
    _ht = p.get("building_height_ft")
    _stories = _num(p, "num_stories")
    _units = _num(p, "residential_units")
    _keys = _num(p, "hotel_rooms")
    _gsf = _num(p, "total_gsf")
    _cost = p.get("permit_cost")

    st.markdown(
        f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin:14px 0 12px">'
        + _tile("Height", f"{_ht:,.0f} ft" if pd.notna(_ht) and _ht else None)
        + _tile("Stories", f"{_stories:,}" if _stories else None)
        + _tile("Residential units",
                ("None" if _units == 0 else f"{_units:,}") if _units is not None else None)
        + _tile("Hotel keys", f"{_keys:,}" if _keys else None)
        + _tile("Total SF", f"{_gsf:,}" if _gsf else None)
        + _tile("Construction cost", money_millions(_cost) or None)
        + '</div>',
        unsafe_allow_html=True,
    )

    # ── 4. Two equal cards: the team, and the site ──────────────────
    _dev_name = title_case(p.get("developer_canonical") or p.get("developer") or "")
    # Kept as its own name because the DEVELOPER SOURCES expander further down
    # keys on it. Losing this definition in the panel rewrite is what raised a
    # NameError on every project click.
    _method = resolution_method(p.get("developer_resolution_method", ""))
    _conf = p.get("developer_confidence") or developer_confidence(
        p.get("developer_resolution_method", ""), _dev_name or "")
    if _dev_name:
        _dev_name = (confidence_dot(_conf, p.get("developer_resolution_method", ""))
                     + _dev_name)

    _addr, _permit_addr = addresses(p)
    _zoning = p.get("zoning_district_raw") or p.get("zoning_raw") or None
    _delivery = _delivered_detail(p) or _target_detail(p) or None

    c1, c2 = st.columns(2)
    c1.markdown(_card("Project team", [
        ("Developer", _dev_name),
        ("Owner entity", title_case(p.get("owner_or_agency") or "")),
        ("Architect", title_case(p.get("architect") or "")),
        ("General contractor", title_case(p.get("contractor") or "")),
        ("Civil engineer", title_case(p.get("civil_engineer") or "")),
        ("Lender", _lender_detail(p)),
    ]), unsafe_allow_html=True)
    c2.markdown(_card("Site", [
        ("Address", _addr),
        ("Permit address", _permit_addr),
        ("Neighborhood", p.get("neighborhood")),
        ("Parcel ID", _parcel_detail(p)),
        ("Zoning", _zoning),
        ("Expected delivery", _delivery),
    ]), unsafe_allow_html=True)

    # ── 5. Programme chips ──────────────────────────────────────────
    # The permit's work-item list, made readable. The asset class is a badge
    # in the header and is NOT repeated with its raw use list beside it, which
    # is what made the old panel unreadable.
    _chips = programme_chips(p.get("asset_class_raw"))
    if _chips:
        st.markdown(
            f'<div style="margin:12px 0 4px;font-family:{_UI};font-size:9px;'
            f'font-weight:700;letter-spacing:0.16em;color:{_MUTED};'
            f'text-transform:uppercase">Program</div>'
            f'<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px">'
            + "".join(_chip(c) for c in _chips) + '</div>',
            unsafe_allow_html=True,
        )

    # ── 6. The raw permit text, collapsed; its only appearance ──────
    _prows = [(lab, val) for lab, val in permit_rows(p) if clean(val)]
    if _prows or p.get("description"):
        with st.expander("PERMIT DETAILS"):
            if _prows:
                # The record itself, rendered here rather than linked to as
                # raw JSON. Built from stored fields, so opening this makes no
                # network call and nothing can be missing because a service is
                # down.
                st.markdown(
                    f'<div style="border:1px solid {_BORDER};background:{_BG2}">'
                    + "".join(
                        f'<div style="display:flex;gap:12px;padding:7px 14px;'
                        f'border-bottom:1px solid rgba(30,37,48,0.75)">'
                        f'<div style="flex:0 0 170px;font-family:{_UI};font-size:8.5px;'
                        f'font-weight:700;letter-spacing:0.12em;color:{_MUTED};'
                        f'text-transform:uppercase;padding-top:3px">{lab}</div>'
                        f'<div style="flex:1;font-family:{_UI};font-size:12.5px;'
                        f'color:#e2e8f0;line-height:1.5">{val}</div></div>'
                        for lab, val in _prows)
                    + '</div>',
                    unsafe_allow_html=True,
                )
            if p.get("description"):
                st.markdown(
                    f'<div style="font-family:{_UI};font-size:8.5px;font-weight:700;'
                    f'letter-spacing:0.12em;color:{_MUTED};text-transform:uppercase;'
                    f'margin:14px 0 6px">Full permit description</div>'
                    f'<div style="font-family:Inter,sans-serif;font-size:12.5px;'
                    f'color:{_MUTED};line-height:1.6">{p["description"]}</div>',
                    unsafe_allow_html=True,
                )

    # Notes are the researcher's caveats and belong with the project, not
    # buried with the permit text. Shown for every market, not just Cambridge.
    _notes_html = ""
    if p.get("notes"):
        _notes_html += f'<div style="margin-bottom:8px">{p["notes"]}</div>'
    if p.get("parking_notes"):
        _notes_html += (f'<div><span style="color:{_MUTED};font-weight:700;'
                        f'letter-spacing:0.1em">PARKING: </span>'
                        f'{p["parking_notes"]}</div>')
    if _notes_html:
        with st.expander("NOTES"):
            st.markdown(
                f'<div style="font-family:Inter,sans-serif;font-size:12.5px;'
                f'color:#cbd5e1;line-height:1.6">{_notes_html}</div>',
                unsafe_allow_html=True,
            )

    # Cambridge Development Log fields
    if is_cambridge:
        far_str = None
        if pd.notna(p.get("far")) and p.get("far"):
            scope_note = " (whole PUD)" if p.get("far_scope") == "pud" else ""
            far_str = f'{p["far"]:.2f}{scope_note}'
        lot_area_str = f'{int(p["lot_area"]):,} SF' if pd.notna(p.get("lot_area")) and p.get("lot_area") else None
        affordable_str = "TBD" if p.get("affordable_units_tbd") else (
            f'{int(p["affordable_units"]):,}' if pd.notna(p.get("affordable_units")) and p.get("affordable_units") else None
        )

        st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
        cc1, cc2, cc3 = st.columns(3)
        with cc1:
            st.markdown(
                _kv("PERMIT TYPE",  p.get("permit_type")) +
                _kv("PROJECT TYPE", p.get("project_type")) +
                _kv("ZONING",       p.get("zoning_raw")),
                unsafe_allow_html=True,
            )
        with cc2:
            st.markdown(
                _kv("LOT AREA", lot_area_str) +
                _kv("FAR",      far_str) +
                _kv("AFFORDABLE UNITS", affordable_str),
                unsafe_allow_html=True,
            )
        with cc3:
            permits = load_cambridge_permits(int(p["id"]))
            special_str = ", ".join(
                f'{s["base"]} {s["amendment"]}' if s["amendment"] else s["base"]
                for s in permits["special_permits"]
            ) or None
            building_str = ", ".join(
                f'{b["number"]} ({b["label"]})' if b["label"] else b["number"]
                for b in permits["building_permits"]
            ) or None
            st.markdown(
                # Hotel keys now print beside the residential unit count in
                # the main panel for every market, so they are not repeated
                # here.
                _kv("SPECIAL PERMIT",  special_str) +
                _kv("BUILDING PERMIT", building_str),
                unsafe_allow_html=True,
            )

        if permits["aliases"]:
            st.markdown(
                _kv("FORMERLY", " → ".join(permits["aliases"] + [p["name"]])),
                unsafe_allow_html=True,
            )

        if p.get("parent_project_id"):
            parent_rows = df[df["id"] == p["parent_project_id"]]
            if not parent_rows.empty:
                st.markdown(
                    _kv("PART OF", f'{p.get("phase_group") or ""} — {parent_rows.iloc[0]["name"]}'),
                    unsafe_allow_html=True,
                )
        elif p.get("phase_group"):
            st.markdown(_kv("PHASE GROUP", p["phase_group"]), unsafe_allow_html=True)

    # Corroborating sources for an inferred developer name. Stored in full,
    # not just the first, so the attribution can actually be audited.
    if _dev_name and _method in ("web_corroborated", "web_low_confidence"):
        try:
            _srcs = json.loads(p.get("developer_sources") or "[]")
        except (ValueError, TypeError):
            _srcs = []
        if _srcs:
            with st.expander(f"DEVELOPER SOURCES  ({len(_srcs)})"):
                st.caption(RESOLUTION_METHODS[_method]["note"])
                for s in _srcs:
                    pub = s.get("publisher") or s.get("domain", "")
                    url = s.get("url", "")
                    st.markdown(f"**{pub}** — [{url}]({url})")
                    st.markdown(f"*Address:* {s.get('address_sentence', '—')}")
                    st.markdown(f"*Developer:* {s.get('developer_sentence', '—')}")
                    st.divider()

    # Per-field citations. Every extracted value says which filing it came
    # from, so a figure can be traced without leaving the row.
    _cites = load_field_citations(int(p["id"]))
    if _cites:
        with st.expander(f"FIELD SOURCES  ({len(_cites)})"):
            st.caption("Where each extracted value came from. A field with no row here "
                       "was not stated in the filing.")
            st.dataframe(
                pd.DataFrame([{
                    "FIELD": c["field"],
                    "VALUE": c["value"],
                    "SOURCE": c["filing"],
                    "DATE": c["date"],
                    "URL": c["url"],
                } for c in _cites]),
                use_container_width=True, hide_index=True,
                column_config={"URL": st.column_config.LinkColumn("URL", display_text="open ↗")},
            )

    # Links
    # A button must open a PAGE FOR A PERSON. The row's own source URL is
    # sometimes the machine endpoint the data came from -- Miami's 309 permit
    # URLs are ArcGIS FeatureServer queries that render as raw JSON -- so any
    # data endpoint is dropped in favour of the agency's public lookup page,
    # and the record itself is shown in PERMIT DETAILS above instead.
    _city = p.get("city") or ""
    _src_url = p.get("permit_url") or p.get("bpda_url") or ""
    if is_data_endpoint(_src_url) or str(_src_url).startswith(("manual:", "sofl://")):
        _src_url = ""

    _lookup = PERMIT_LOOKUP.get(_city)
    _prop = property_lookup(_city)
    lc1, lc2, lc3, _ = st.columns([1.2, 1, 1, 2.8])
    if _src_url:
        lc1.link_button(f"{source_label(_city, _src_url).upper()} ↗", _src_url)
    elif _lookup:
        lc1.link_button(f"{_lookup[0].upper()} ↗", _lookup[1])
    elif is_cambridge:
        lc1.link_button("SEARCH SPECIAL PERMITS ↗", "https://www.cambridgema.gov/specialpermits")
    if _prop and p.get("parcel_id"):
        lc2.link_button(f"{_prop[0].upper()} ↗", _prop[1])
    if p.get("processed_filing_url"):
        lc3.link_button(f"SOURCE {(p.get('processed_filing_type') or 'PDF').upper()} ↗",
                        p["processed_filing_url"])

    # Both lookup pages search by an identifier rather than accepting a deep
    # link -- iBuild needs an account and the Property Appraiser's folio route
    # does not resolve -- so the identifiers sit beside the buttons with
    # st.code's own copy control.
    if _lookup or _prop:
        _ids = [(lab, val) for lab, val in
                (("Process number", p.get("permit_process_number")),
                 ("Permit number", p.get("permit_number")),
                 ("Folio", p.get("parcel_id"))) if clean(val)]
        if _ids:
            st.caption("iBuild searches by process or permit number; the Property "
                       "Appraiser searches by folio. Copy either here.")
            for col, (lab, val) in zip(st.columns(len(_ids)), _ids):
                col.caption(lab)
                col.code(str(val), language=None)

    # Filings
    filings_df = load_filings(int(p["id"]))
    if not filings_df.empty:
        with st.expander(f"ALL FILINGS  ({len(filings_df)})"):
            st.dataframe(filings_df, use_container_width=True, hide_index=True)

    st.markdown(f'<div style="height:1px;background:{_BORDER};margin:16px 0 8px"></div>',
                unsafe_allow_html=True)
