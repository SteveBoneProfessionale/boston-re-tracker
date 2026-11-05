r"""Read the Florida Division of Corporations BULK data files.

    python -m scraper.acq_sofl_sunbiz_bulk --fetch     # download from sftp
    python -m scraper.acq_sofl_sunbiz_bulk --index     # build the entity index

WHY BULK RATHER THAN THE SEARCH SITE. search.sunbiz.org sits behind Cloudflare
and challenged this work twice mid-run; nothing here tries to defeat that. The
Division publishes the same register as free data files and documents exactly
how to reach them, so this takes the front door:

    Host      sftp.floridados.gov          (SFTP, port 22)
    Username  Public
    Password  PubAccess1845!
    Quarterly /Public/doc/Quarterly/Cor/cordata.zip   (all active entities)
    Daily     /Public/doc/cor/YYYYMMDDc.txt           (that day's filings)

Those credentials are printed on the Division's own Data Downloads page beside
the words "free data downloads"; they are a published anonymous login, not a
secret. The host key is pinned in the usual way through known_hosts.

THE QUARTERLY FILE IS A SNAPSHOT, THE DAILIES ARE THE DELTA. The quarterly is
generated in January, April, July and October; anything filed or amended since
then is only in the daily files. Both are read, dailies last, so a later filing
overwrites the snapshot -- which matters here because several of these entities
were formed in 2026 specifically to hold the property they bought.

RECORD FORMAT: fixed-width ASCII, 1,440 characters, up to six officers per
entity starting at position 669 with a 128-character stride. Two fields carry
the weight for this work:

    Officer Type  "P" a natural person, "C" a corporation
    Officer Title a four-character code -- MGR, MGRM, AMBR, GP and so on

The REGISTERED AGENT IS A SEPARATE FIELD (31-36) and is never read into the
officer list. That is a structural guarantee, not a filter: an agent, a law
firm or an agent service cannot be mistaken for a principal here because the
code never looks at that part of the record.
"""

import argparse
import io
import os
import re
import subprocess
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

CACHE = os.path.join(ROOT, "data", "cache", "sunbiz_bulk")
SFTP = "sftp://sftp.floridados.gov"
LOGIN = "Public:PubAccess1845!"
QUARTERLY = "/Public/doc/Quarterly/Cor/cordata.zip"
DAILY_DIR = "/Public/doc/cor/"

RECORD_LEN = 1440
OFFICER_BASE = 668          # zero-based; the layout counts from 1
OFFICER_STRIDE = 128
OFFICER_SLOTS = 6

# Four-character title codes that mean "runs the company". The documented list
# covers corporate officers only (P, T, C, V, S, D); the LLC and partnership
# codes below are what the data actually carries. Anything outside this set --
# and the registered agent, which is not an officer at all -- is not a
# principal.
MANAGER_TITLES = {
    "MGR", "MGRM", "MBR", "AMBR", "MEM", "AP",      # LLC
    "GP", "GENP", "PTR", "PART",                     # partnership
    "P", "PRES", "CEO", "MP",                        # corporate, running it
}


def _field(rec, start, length):
    """One field, by the layout's 1-based start position."""
    return rec[start - 1:start - 1 + length].strip()


def parse_record(rec):
    """One fixed-width corporate record as a dict.

    Shaped to match what the website parser returned, so the resolver reads
    either source without knowing which it has.
    """
    out = {
        "document_number": _field(rec, 1, 12),
        "name": _field(rec, 13, 192),
        "status": _field(rec, 205, 1),
        "entity_type": _field(rec, 206, 15),
        "principal_address": ", ".join(p for p in (
            _field(rec, 221, 42), _field(rec, 263, 42), _field(rec, 305, 28),
            _field(rec, 333, 2), _field(rec, 335, 10)) if p),
        "mailing_address": ", ".join(p for p in (
            _field(rec, 347, 42), _field(rec, 389, 42), _field(rec, 431, 28),
            _field(rec, 459, 2), _field(rec, 461, 10)) if p),
        "date_filed": _field(rec, 473, 8),
        "fei": _field(rec, 481, 14),
        "state": _field(rec, 504, 2),
        # Carried so a caller can SEE that it was available and deliberately
        # not treated as a principal.
        "registered_agent": _field(rec, 545, 42),
        "more_than_six_officers": _field(rec, 495, 1) == "Y",
        "persons": [],
    }
    for i in range(OFFICER_SLOTS):
        base = OFFICER_BASE + i * OFFICER_STRIDE
        title = rec[base:base + 4].strip()
        kind = rec[base + 4:base + 5].strip()
        name = rec[base + 5:base + 47].strip()
        addr = ", ".join(p for p in (rec[base + 47:base + 89].strip(),
                                     rec[base + 89:base + 117].strip(),
                                     rec[base + 117:base + 119].strip()) if p)
        if not name:
            continue
        out["persons"].append({
            "title": title,
            "name": name,
            "address": addr or None,
            # "P" natural person, "C" corporation.
            "is_person": kind.upper() == "P",
            "is_manager": title.upper() in MANAGER_TITLES,
        })
    return out


def norm(name):
    return re.sub(r"[^A-Z0-9]", "", str(name or "").upper())


def _curl(path, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    cmd = ["curl", "-s", "--max-time", "5400", "-u", LOGIN,
           SFTP + path, "-o", dest]
    subprocess.run(cmd, check=True, timeout=5700)
    return dest


def fetch(verbose=True):
    """Download the quarterly snapshot and every daily since it was cut."""
    os.makedirs(CACHE, exist_ok=True)
    quarterly = os.path.join(CACHE, "cordata.zip")
    if not os.path.exists(quarterly):
        if verbose:
            print("downloading the quarterly snapshot (~1.8 GB)...",
                  file=sys.stderr)
        _curl(QUARTERLY, quarterly)

    listing = subprocess.run(
        ["curl", "-s", "--max-time", "180", "-u", LOGIN, SFTP + DAILY_DIR],
        capture_output=True, timeout=300).stdout.decode("utf-8", "replace")
    dailies = sorted(re.findall(r"(\d{8}c\.txt)", listing))
    got = []
    for name in dailies:
        dest = os.path.join(CACHE, name)
        if not os.path.exists(dest):
            _curl(DAILY_DIR + name, dest)
        got.append(dest)
    if verbose:
        print(f"quarterly + {len(got)} daily files in {CACHE}", file=sys.stderr)
    return quarterly, got


def _records(stream):
    """Yield fixed-width records from a text stream.

    The files are newline-delimited in practice, but a record is defined by
    its LENGTH rather than by a terminator, so a line that is short is padded
    and anything longer is split -- which keeps one malformed line from
    shifting every field after it.
    """
    for line in stream:
        line = line.rstrip("\r\n")
        if len(line) < 200:
            continue
        if len(line) < RECORD_LEN:
            line = line.ljust(RECORD_LEN)
        for i in range(0, len(line) - RECORD_LEN + 1, RECORD_LEN):
            yield line[i:i + RECORD_LEN]


def index(wanted, verbose=True):
    """{normalised name -> record} for the names asked for.

    Scans rather than loads: the snapshot is some eight gigabytes expanded and
    only a hundred-odd entities are of interest, so records are matched and
    discarded as they stream past.
    """
    want = {norm(w) for w in wanted if norm(w)}
    found = {}
    quarterly = os.path.join(CACHE, "cordata.zip")

    if os.path.exists(quarterly):
        # STREAMED THROUGH unzip, NOT zipfile. The Division compresses these
        # with DEFLATE64, which Python's zipfile refuses outright --
        # "NotImplementedError: That compression method is not supported" --
        # while Info-ZIP reads it happily. `unzip -p` writes one member to
        # stdout, so the ~19 GB expanded never touches disk and is matched and
        # discarded a record at a time.
        with zipfile.ZipFile(quarterly) as zf:
            members = zf.namelist()
        for member in members:
            if verbose:
                print(f"  scanning {member}", file=sys.stderr)
            proc = subprocess.Popen(["unzip", "-p", quarterly, member],
                                    stdout=subprocess.PIPE)
            try:
                stream = io.TextIOWrapper(proc.stdout, encoding="latin-1",
                                          errors="replace")
                for n, rec in enumerate(_records(stream), 1):
                    key = norm(rec[12:204])
                    if key in want:
                        found[key] = parse_record(rec)
                    if verbose and n % 2_000_000 == 0:
                        print(f"    {n:,} records, {len(found)} matched",
                              file=sys.stderr)
            finally:
                proc.stdout.close()
                proc.wait()

    # Dailies last, so a 2026 filing beats the snapshot.
    for name in sorted(os.listdir(CACHE)):
        if not re.fullmatch(r"\d{8}c\.txt", name):
            continue
        with open(os.path.join(CACHE, name), encoding="latin-1",
                  errors="replace") as fh:
            for rec in _records(fh):
                key = norm(rec[12:204])
                if key in want:
                    found[key] = parse_record(rec)

    if verbose:
        print(f"{len(found)} of {len(want)} names found in the bulk data",
              file=sys.stderr)
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--index", action="store_true")
    args = ap.parse_args()
    if args.fetch:
        fetch()
    if args.index:
        import json
        import sqlite3
        from scraper import acq_sofl_resolve as R
        conn = sqlite3.connect(os.path.join(ROOT, "data", "acquisitions.db"))
        names = set()
        for buyer, seller in conn.execute(
                "select buyer, seller from transactions "
                "where source in ('miamidade_pa','pbcpao')"):
            for v in (buyer, seller):
                if v and v.strip():
                    names.add(v.strip())
                    names.update(R.base_names(v.strip()))
        out = index(names)
        dest = os.path.join(ROOT, "data", "cache", "sunbiz_bulk_index.json")
        with open(dest, "w", encoding="utf-8") as fh:
            json.dump(out, fh, indent=1)
        print(f"wrote {dest} with {len(out)} records")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
