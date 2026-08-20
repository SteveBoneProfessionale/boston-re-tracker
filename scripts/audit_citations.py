"""Check that every stored value is actually supported by its stored quote.

A citation that does not contain the firm it is supposed to evidence is not a
citation. This compares each live resolved value against its firm_sentence and
reports the ones where the quote does not carry the name, so they can be
demoted rather than passing as document-confirmed.

Matching is deliberately loose -- initials, punctuation and case vary between
a title block and a tracker row -- so a flag here means the quote really does
not mention the firm, not merely that it spells it differently.
"""
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from addr_norm import norm_firm

STOP = {"the", "and", "inc", "llc", "llp", "ltd", "co", "corp", "company",
        "associates", "group", "partners", "architects", "architect",
        "engineering", "engineers", "design", "studio", "construction"}


def tokens(name):
    return [t for t in norm_firm(name).split() if t and t not in STOP and len(t) > 2]


def supported(value, quote):
    if not value or not quote:
        return None                      # nothing to check against
    q = norm_firm(quote)
    toks = tokens(value)
    if not toks:
        # Nothing distinctive left; fall back to any word of 4+ characters.
        toks = [t for t in norm_firm(value).split() if len(t) > 3]
    if not toks:
        return None
    hit = sum(1 for t in toks if t in q)
    return hit >= max(1, len(toks) // 2)


def main():
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    rows = c.execute("""select fp.*, p.address, p.city from field_provenance fp
                        join projects p on p.id = fp.project_id
                        where fp.superseded = 0 and fp.outcome = 'resolved'
                        and fp.tier in ('document_confirmed','registry_confirmed',
                                        'web_corroborated','web_low_confidence')
                     """).fetchall()
    checked = unsupported = noquote = 0
    bad = []
    for r in rows:
        v = supported(r["value"], r["firm_sentence"])
        if v is None:
            noquote += 1
            continue
        checked += 1
        if not v:
            unsupported += 1
            bad.append(r)
    print(f"resolved values with a tier: {len(rows)}")
    print(f"  checked against a stored quote: {checked}")
    print(f"  no quote stored (structured record): {noquote}")
    print(f"  quote does NOT contain the value:   {unsupported}")
    print()
    for r in bad:
        print(f"  {r['project_id']:4} {str(r['city'])[:10]:10} {str(r['address'])[:24]:24} "
              f"{r['field'][:16]:16} {str(r['value'])[:28]:28} [{r['tier']}] {r['source_type']}")
        print(f"       quote: \"{str(r['firm_sentence'])[:120]}\"")


if __name__ == "__main__":
    main()
