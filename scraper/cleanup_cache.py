"""One-off: remove bad AI cache entries and reset affected project canonical fields."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import DeveloperCache, Project

init_db()
s = get_session()

BAD_PHRASES = [
    "i don't have",
    "i cannot",
    "without access",
    "cannot reliably",
    "cannot identify",
    "unable to",
    "no access to",
]

bad = s.query(DeveloperCache).filter(DeveloperCache.resolved_by == "ai").all()
cleaned = 0
for c in bad:
    low = c.canonical_name.lower()
    if any(p in low for p in BAD_PHRASES) or len(c.canonical_name) > 80:
        s.delete(c)
        cleaned += 1
s.commit()
print(f"Deleted {cleaned} bad cache entries")

reset = 0
for p in s.query(Project).filter(Project.developer_canonical.isnot(None)).all():
    val = p.developer_canonical or ""
    low = val.lower()
    if any(ph in low for ph in BAD_PHRASES) or len(val) > 80:
        p.developer_canonical = None
        reset += 1
s.commit()
print(f"Reset {reset} projects with bad canonical values")
s.close()
