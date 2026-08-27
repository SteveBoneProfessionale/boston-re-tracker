"""Data access for the Acquisitions tab.

WHY THIS FILE EXISTS, since the obvious home for it is app/data.py alongside
load_projects and load_news.

The live deploy failed with

    ImportError: cannot import name 'load_transactions' from 'app.data'
                 (/mount/src/boston-re-tracker/app/data.py)

while that exact function sat at line 1016 of that exact file on GitHub, in a
tree byte-identical to local, and while `from app.data import load_projects,
load_news, summary_stats` one line earlier in app/main.py SUCCEEDED. So the
module was importing fine and simply did not contain a name the source defines
at column zero, unconditionally. There was no "partially initialized module" in
the error, so it was not a circular import.

The only thing that produces that: Streamlit Cloud's checkout of app/data.py was
stale. Cloud pulls into a persistent clone rather than cloning fresh, and that
clone had an older app/data.py -- predating the Acquisitions work -- while
serving current versions of everything else. Rebooting and clearing the cache
did not shift it.

A file the deployment has NEVER SEEN cannot be stale, because there is no older
copy for it to keep. Moving the function to a new module routes around a wedged
checkout deterministically instead of hoping the next pull behaves. That is the
entire reason for the split, and if the Cloud clone is ever rebuilt from scratch
this could fold back into app/data.py with no loss.

app/data.py keeps its own definition untouched, so any other caller of
`app.data.load_transactions` continues to work.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st

# Columns that must come back as numbers rather than text. The grid sorts on
# dtype, so a numeric column left as object sorts "9" above "1000" -- the bug
# that bit the screener on square footage.
_NUMERIC = (
    "price", "implied_valuation", "building_sf", "unit_count", "land_sf",
    "price_per_sf", "price_per_unit", "pct_acquired", "excise_stamp",
    "excise_implied_price",
)


@st.cache_data(ttl=300)
def load_transactions() -> pd.DataFrame:
    """Commercial transactions for the Acquisitions tab.

    Numeric columns come back numeric and the date comes back a date, so the
    grid's own header sort orders by magnitude and chronology rather than by
    text.
    """
    from sqlalchemy import text

    from db.database import engine

    with engine.connect() as conn:
        # Quarantined rows are affiliated-party transfers -- a company conveying
        # to itself. They are not acquisitions and must not enter any count,
        # volume or ranking, so they are excluded HERE rather than filtered in
        # each consumer, where one missed filter would silently reinstate them.
        # They remain in the table with their reason recorded, for review.
        rows = conn.execute(text(
            "select * from transactions where coalesce(quarantined,0) = 0"
        )).mappings().all()

    df = pd.DataFrame([dict(r) for r in rows])
    if df.empty:
        return df
    for c in _NUMERIC:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "sale_date" in df.columns:
        df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    return df
