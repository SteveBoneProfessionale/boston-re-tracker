r"""Completion dates that keep the precision their source actually gave.

A delivery date is almost never a day. Sources say "2026", "Q3 2027",
"December 2026", "Spring 2024" -- and the previous field stored all of that as
free text, which meant it could not be sorted and could not be compared. The
fix is not to invent a day. It is to store two things: a real date for the
START of whatever period the source named, and the precision of that period,
so the value sorts chronologically and still renders as what was said.

    "Q2 2026"       -> date(2026, 4, 1),  precision "quarter",  shows "Q2 2026"
    "2025"          -> date(2025, 1, 1),  precision "year",     shows "2025"
    "December 2026" -> date(2026,12, 1),  precision "month",    shows "Dec 2026"
    "2026-04-15"    -> date(2026, 4,15),  precision "day",      shows "15 Apr 2026"

Sorting uses the date, display uses the precision. Rendering a quarter as
2026-04-01 would be a false specific date and is exactly what this prevents.

Ranges and seasons are recorded, not guessed at. "2024-2025" gets the start of
the range and a note saying so; "Spring 2024" gets the quarter the season falls
in and a note naming the word that produced it. The note travels into the
provenance row, so nobody later reads Q2 2024 as something a source wrote.
"""

import re
from datetime import date

PRECISIONS = ("day", "month", "quarter", "year")

# Coarsest first. Used when two claims about one project have to be compared:
# a more precise source supersedes a vaguer one for the same period.
PRECISION_RANK = {"year": 0, "quarter": 1, "month": 2, "day": 3}

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12,
    "december": 12,
}
_MONTH_ABBR = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# A season is a quarter with the word recorded, never a month. "Spring 2024"
# names three months and the source meant all three; picking April would be
# inventing precision the source did not have.
_SEASONS = {"winter": 1, "spring": 2, "summer": 3, "fall": 4, "autumn": 4}

# Qualifiers that colour a year without narrowing it to a quarter. "Late 2025"
# is somebody's adjective, not Q4, so it stays year precision.
_VAGUE = re.compile(r"\b(early|mid|late|end of|beginning of|by|circa|around|about)\b", re.I)

_Y = r"(?:19|20)\d{2}"          # non-capturing on purpose: findall stays clean
_DAYS_IN = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def _quarter_start(year: int, q: int) -> date:
    return date(year, 3 * (q - 1) + 1, 1)


def parse_date_phrase(text) -> tuple[date, str, str] | None:
    """Parse a completion phrase into (date, precision, note).

    Returns None when nothing date-like is present. The note is empty unless
    something was inferred -- a season mapped to a quarter, a range reduced to
    its start -- in which case it says exactly what happened, so the inference
    is auditable rather than invisible.
    """
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None

    # An exact day first: this is what a certificate of occupancy or a permit
    # final gives, and it is the only case where a day is real.
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", s)
    if m:
        try:
            return date(*(int(g) for g in m.groups())), "day", ""
        except ValueError:
            return None
    m = re.search(rf"\b([A-Za-z]+)\.?\s+(\d{{1,2}}),?\s+({_Y})\b", s)
    if m and m.group(1).lower() in _MONTHS:
        try:
            return date(int(m.group(3)), _MONTHS[m.group(1).lower()],
                        int(m.group(2))), "day", ""
        except ValueError:
            pass
    m = re.search(rf"\b(\d{{1,2}})\s+([A-Za-z]+)\.?,?\s+({_Y})\b", s)
    if m and m.group(2).lower() in _MONTHS:
        try:
            return date(int(m.group(3)), _MONTHS[m.group(2).lower()],
                        int(m.group(1))), "day", ""
        except ValueError:
            pass

    # A quarter, written either order. "2026 Q3/Q4" keeps the earlier quarter
    # and says in the note that it was a range.
    found = []
    for mm in re.finditer(rf"\bQ([1-4])\b[^0-9]{{0,8}}({_Y})", s, re.I):
        found.append((int(mm.group(2)), int(mm.group(1))))
    for mm in re.finditer(rf"({_Y})[^0-9]{{0,8}}\bQ([1-4])\b", s, re.I):
        found.append((int(mm.group(1)), int(mm.group(2))))
    if found:
        found.sort()
        year, q = found[0]
        note = ""
        if len(set(found)) > 1 or re.search(r"Q\s*[1-4]\s*[/–-]\s*Q?\s*[1-4]", s, re.I):
            note = (f'source states a range ("{s}"); stored as the start of '
                    f"the earliest period named")
        return _quarter_start(year, q), "quarter", note

    # A season, which is a quarter with its word recorded.
    m = re.search(rf"\b({'|'.join(_SEASONS)})\b[^0-9]{{0,8}}({_Y})", s, re.I)
    if m:
        word = m.group(1).lower()
        return (_quarter_start(int(m.group(2)), _SEASONS[word]), "quarter",
                f'source says "{m.group(0)}"; {word} recorded as '
                f"Q{_SEASONS[word]} because no month was stated")

    # A month and a year.
    m = re.search(rf"\b([A-Za-z]+)\.?\s+({_Y})\b", s)
    if m and m.group(1).lower() in _MONTHS:
        return date(int(m.group(2)), _MONTHS[m.group(1).lower()], 1), "month", ""
    m = re.search(rf"\b({_Y})[-/](0[1-9]|1[0-2])\b", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1), "month", ""

    # A year, or a range of them.
    years = [int(mm.group(0)) for mm in re.finditer(rf"\b{_Y}\b", s)]
    if years:
        note = ""
        if len(set(years)) > 1:
            note = (f'source states a range ("{s}"); stored as the start of '
                    f"the earliest period named")
        elif _VAGUE.search(s):
            note = (f'source says "{s}"; the qualifier does not name a '
                    f"quarter, so the precision stays the year")
        return date(min(years), 1, 1), "year", note
    return None


def format_date(d, precision: str) -> str:
    """Render a stored date at the precision its source actually had.

    Tolerates pandas' NaT without importing pandas: a missing value is the
    only thing that is not equal to itself.
    """
    if d is None or d != d:
        return ""
    p = (precision or "day").lower()
    if p == "year":
        return f"{d.year}"
    if p == "quarter":
        return f"Q{(d.month - 1) // 3 + 1} {d.year}"
    if p == "month":
        return f"{_MONTH_ABBR[d.month]} {d.year}"
    return f"{d.day} {_MONTH_ABBR[d.month]} {d.year}"


def period_end(d, precision: str):
    """The last day the source's period could mean.

    Not stored. Used when deciding whether a forecast has been overtaken by an
    actual delivery: a 2026 target is not wrong on 2 January 2026.
    """
    if d is None:
        return None
    p = (precision or "day").lower()
    if p == "year":
        return date(d.year, 12, 31)
    month = d.month
    if p == "quarter":
        month = 3 * ((d.month - 1) // 3 + 1)
    elif p != "month":
        return d
    last = _DAYS_IN[month]
    if month == 2 and (d.year % 4 == 0 and (d.year % 100 != 0 or d.year % 400 == 0)):
        last = 29
    return date(d.year, month, last)
