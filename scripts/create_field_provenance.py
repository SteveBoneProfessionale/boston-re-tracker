"""field_provenance: one row per (project, field) resolution attempt.

A value now carries seven attributes plus two audit sentences, and the GC
field needs `not_yet_selected` as a state distinct from null. That does not
belong in a widened `projects` table, so it lives here and `projects` keeps
only the resolved value for the app to read.
"""
import sqlite3

DDL = """
CREATE TABLE IF NOT EXISTS field_provenance (
    id                INTEGER PRIMARY KEY,
    project_id        INTEGER NOT NULL REFERENCES projects(id),
    field             VARCHAR NOT NULL,   -- architect | civil_engineer | general_contractor
    value             TEXT,               -- firm name; NULL when unresolved
    outcome           VARCHAR NOT NULL,   -- resolved | null | not_yet_selected
    tier              VARCHAR,            -- document_confirmed | registry_confirmed
                                          -- | web_corroborated | web_low_confidence
    source_type       VARCHAR,            -- article80_pdf | planset | minutes | permit
                                          -- | compliance_report | licence_registry | web
    source_url        TEXT,
    source_name       TEXT,
    source_date       VARCHAR,
    page_ref          VARCHAR,            -- page number or record id
    firm_sentence     TEXT,               -- passage naming the firm in the role
    address_sentence  TEXT,               -- passage establishing the project identity
    resolution_step   INTEGER,            -- 1 audit | 2 documents | 3 registry | 4 web
    reason            TEXT,               -- why null, or why a prior value was rejected
    superseded        BOOLEAN DEFAULT 0,
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_fp_project ON field_provenance(project_id);
CREATE INDEX IF NOT EXISTS ix_fp_field   ON field_provenance(field);
CREATE INDEX IF NOT EXISTS ix_fp_live    ON field_provenance(project_id, field, superseded);

-- Corroboration evidence: one row per independent source backing a claim.
-- web_corroborated requires two rows here from different domains.
CREATE TABLE IF NOT EXISTS field_evidence (
    id                INTEGER PRIMARY KEY,
    project_id        INTEGER NOT NULL REFERENCES projects(id),
    field             VARCHAR NOT NULL,
    value             TEXT NOT NULL,
    source_url        TEXT,
    source_domain     VARCHAR,
    source_title      TEXT,
    source_date       VARCHAR,
    firm_sentence     TEXT,
    address_sentence  TEXT,
    is_aggregator     BOOLEAN DEFAULT 0,
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_fe_claim ON field_evidence(project_id, field, value);

-- Values that were already in `projects` before this run, so a failed
-- verification can be reported and, if needed, reversed.
CREATE TABLE IF NOT EXISTS prior_value_audit (
    id            INTEGER PRIMARY KEY,
    project_id    INTEGER NOT NULL REFERENCES projects(id),
    field         VARCHAR NOT NULL,
    prior_value   TEXT,
    verdict       VARCHAR,   -- confirmed | role_not_labelled | firm_absent | doc_unavailable
    page_ref      VARCHAR,
    firm_sentence TEXT,
    note          TEXT,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_pva ON prior_value_audit(project_id, field);
"""


def main():
    c = sqlite3.connect("data/boston_re.db")
    c.executescript(DDL)
    c.commit()
    for t in ("field_provenance", "field_evidence", "prior_value_audit"):
        n = c.execute(f"select count(*) from {t}").fetchone()[0]
        print(f"{t}: {n} rows")


if __name__ == "__main__":
    main()
