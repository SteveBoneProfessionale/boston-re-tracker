"""Quick database verification — run after the scraper to confirm data looks right."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from db.database import init_db, get_session
from db.models import Project, ProjectFiling

init_db()
session = get_session()

projects = session.query(Project).order_by(Project.name).all()
print(f"\n{'='*90}")
print(f"PROJECTS IN DATABASE ({len(projects)} total)")
print(f"{'='*90}")
print(f"{'NAME':<45} {'STATUS':<22} {'NEIGHBORHOOD':<22} {'ADDR?':>5} {'FILINGS':>7}")
print(f"{'-'*90}")
for p in projects:
    has_addr = "Yes" if p.address else "No"
    print(f"{(p.name or '')[:44]:<45} {(p.status or '')[:21]:<22} "
          f"{(p.neighborhood or '')[:21]:<22} {has_addr:>5} {len(p.filings):>7}")

print(f"\n{'='*90}")
print("FILINGS BY PROJECT")
print(f"{'='*90}")
for p in projects:
    if p.filings:
        print(f"\n{p.name}:")
        for f in p.filings:
            print(f"  [{f.filing_category.upper():>12}]  {f.date}  {f.name[:60]}")
            print(f"              URL: {f.url[:80]}")

session.close()
