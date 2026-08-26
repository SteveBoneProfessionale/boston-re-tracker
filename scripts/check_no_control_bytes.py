r"""Guard against a bug that has now corrupted three regexes silently.

Patching Python source through a bash heredoc turns "\\b" into a literal 0x08
BACKSPACE byte, because "\\\\b" inside a non-raw Python string is the backspace
escape. The result compiles, imports, runs, and matches NOTHING -- and reports
success while doing it. It has hit three files:

    acq_resolve_patterns.py    twelve sponsor patterns matched nothing
    acq_resolve_addresses.py   the guard against absorbing independent
                               institutions never fired, and I reported it as
                               working because I had reverted the bad rows by
                               hand
    acq_date_audit.py          the date detector found no dates at all, so
                               every press row was classified a publication
                               proxy

Each time the failure looked like a legitimate result. That is what makes it
worth a permanent check rather than more care.

    python scripts/check_no_control_bytes.py
"""

import sys
from pathlib import Path

BAD = {0x08: r"\b (backspace)", 0x07: r"\a (bell)", 0x0C: r"\f (formfeed)",
       0x0B: r"\v (vertical tab)"}
ROOTS = ("scraper", "app", "db", "scripts")


def main() -> int:
    failures = []
    for root in ROOTS:
        for p in Path(root).rglob("*.py"):
            raw = p.read_bytes()
            for code, name in BAD.items():
                n = raw.count(bytes([code]))
                if n:
                    failures.append((p, name, n))
    if not failures:
        print("clean: no control bytes in any .py under " + ", ".join(ROOTS))
        return 0
    for p, name, n in failures:
        print(f"FAIL {p}: {n} occurrence(s) of {name} — almost certainly a "
              f"mangled regex escape that will silently match nothing")
    return 1


if __name__ == "__main__":
    sys.exit(main())
