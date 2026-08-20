"""Write resolutions into field_provenance and mirror the value into projects.

Every write goes through record(); nothing else touches the three fields, so
the provenance table is a complete account of how each value got there.
"""
import re
import sqlite3

FIELDS = ("architect", "civil_engineer", "general_contractor")
TIER_RANK = {
    "document_confirmed": 4,
    "registry_confirmed": 4,
    "web_corroborated": 2,
    "web_low_confidence": 1,
}

# A value that looks like an individual rather than a firm. Permit datasets
# routinely put a person in a field named for a company.
FIRM_WORD = re.compile(
    r"architect|engineer|design|studio|associate|partner|group|inc\b|llc|llp|"
    r"corp|company|\bco\b|assoc|atelier|works|collaborative|consult|"
    r"builders|construction|contracting|development|\bpc\b|\bpa\b|office",
    re.I)


def looks_like_person(name):
    if not name:
        return False
    s = str(name).strip()
    if FIRM_WORD.search(s):
        return False
    toks = [t for t in re.split(r"\s+", s) if t]
    if not 2 <= len(toks) <= 4:
        return False
    # An initialism is a firm, not a given name: "CDM SMITH", "ZGF", "BWA".
    if any(re.fullmatch(r"[A-Z]{2,4}", t) and not re.search(r"[AEIOU]", t)
           for t in toks):
        return False
    # ALL CAPS "BRETT D LAMBERT" or Title Case "Daniel P Anderson"
    return all(re.fullmatch(r"[A-Za-z][A-Za-z.'\-]*", t) for t in toks)


def connect(path="data/boston_re.db"):
    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    return c


def record(c, project_id, field, *, value=None, outcome="resolved", tier=None,
           source_type=None, source_url=None, source_name=None, source_date=None,
           page_ref=None, firm_sentence=None, address_sentence=None,
           resolution_step=None, reason=None, mirror=True):
    """Insert a provenance row, superseding any earlier live row for the pair."""
    assert field in FIELDS, field
    assert outcome in ("resolved", "null", "not_yet_selected"), outcome
    # Precedence. The waterfall stops at the first step that produces an
    # answer, so a step that produced NO answer must never displace one that
    # did -- an Article 80 filing saying "a GC will be retained" cannot
    # unseat a compliance report that names the builder.
    cur_live = c.execute(
        "select * from field_provenance where project_id=? and field=? "
        "and superseded=0", (project_id, field)).fetchone()
    demote_new = False
    if cur_live is not None:
        held, incoming = cur_live["outcome"] == "resolved", outcome == "resolved"
        if held and not incoming:
            demote_new = True
        elif held and incoming:
            # Both answered. The earlier waterfall step wins; on a tie, the
            # stronger tier. A genuine disagreement is kept, not silently lost.
            old_step = cur_live["resolution_step"] or 9
            new_step = resolution_step or 9
            if old_step < new_step:
                demote_new = True
            elif old_step == new_step and                     TIER_RANK.get(tier, 0) < TIER_RANK.get(cur_live["tier"], 0):
                demote_new = True
            if (cur_live["value"] or "").strip().lower() != (value or "").strip().lower():
                note = (f"conflicts with {'superseded' if not demote_new else 'retained'} "
                        f"{cur_live['source_type']} value "
                        f"\"{cur_live['value']}\" (step {old_step}, {cur_live['tier']})")
                reason = f"{reason} | {note}" if reason else note
    if not demote_new:
        c.execute("update field_provenance set superseded=1 "
                  "where project_id=? and field=? and superseded=0",
                  (project_id, field))
    cur = c.execute("""insert into field_provenance
        (project_id, field, value, outcome, tier, source_type, source_url,
         source_name, source_date, page_ref, firm_sentence, address_sentence,
         resolution_step, reason, superseded)
        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (project_id, field, value, outcome, tier, source_type, source_url,
               source_name, source_date, page_ref, firm_sentence, address_sentence,
               resolution_step, reason, 1 if demote_new else 0))
    rowid = cur.lastrowid
    if mirror and not demote_new:
        val = value if outcome == "resolved" else (
            "not_yet_selected" if outcome == "not_yet_selected" else None)
        c.execute(f"update projects set {field}=? where id=?", (val, project_id))
    return rowid


def add_evidence(c, project_id, field, value, *, source_url=None, source_domain=None,
                 source_title=None, source_date=None, firm_sentence=None,
                 address_sentence=None, is_aggregator=0):
    c.execute("""insert into field_evidence
        (project_id, field, value, source_url, source_domain, source_title,
         source_date, firm_sentence, address_sentence, is_aggregator)
        values (?,?,?,?,?,?,?,?,?,?)""",
              (project_id, field, value, source_url, source_domain, source_title,
               source_date, firm_sentence, address_sentence, is_aggregator))


def audit_prior(c, project_id, field, prior_value, verdict, *, page_ref=None,
                firm_sentence=None, note=None):
    c.execute("""insert into prior_value_audit
        (project_id, field, prior_value, verdict, page_ref, firm_sentence, note)
        values (?,?,?,?,?,?,?)""",
              (project_id, field, prior_value, verdict, page_ref, firm_sentence, note))


def live(c, project_id, field):
    return c.execute("select * from field_provenance where project_id=? and field=? "
                     "and superseded=0", (project_id, field)).fetchone()


def construction_started(row):
    """Whether a GC should be expected to exist yet."""
    return (row["completion_stage"] in ("Under Construction", "Complete")
            or row["status"] in ("Under Construction", "Complete",
                                 "Building Permit Granted"))
