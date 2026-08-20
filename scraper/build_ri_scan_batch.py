"""Read the image-only Rhode Island board documents with vision.

Fifty-five of the matched documents carry no extractable text -- they are
scans of recorded zoning-board resolutions and site plans. Tesseract is not
installed here, so the pages are rendered and read directly instead.

Resolution is kept modest on purpose: these are typed decisions, not fine
drawings, and the cost scales with pixels.
"""
import base64
import io
import json
import sys
from pathlib import Path

import fitz

sys.path.insert(0, str(Path(__file__).parent))

PLANSETS = Path("data/ri_plansets")
OUT = Path("data/ri_scan_requests.jsonl")
IDMAP = Path("data/ri_scan_idmap.json")
MODEL = "claude-haiku-4-5-20251001"
MAX_PAGES = 5
DPI = 110

SYSTEM = """You read scanned Rhode Island zoning-board and plan-commission
documents -- recorded resolutions, decisions, site plans and their title
blocks -- and report the subject property and the project team.

Rules, all absolute:
- Take a firm or person ONLY where the document ties them to that role.
  "Plans prepared by Smith Architects" and a title block reading
  "CIVIL ENGINEER: DiPrete Engineering" are answers. An attorney appearing for
  the applicant is not an architect. A name in an attendance list is not an
  answer.
- An engineering firm is never the architect and an architecture firm is never
  the civil engineer.
- A landscape architect is not the architect. A surveyor is not the civil
  engineer. A traffic engineer is not the civil engineer.
- If only a person is named, give the person and leave firm null. Never guess
  their employer.
- Transcribe quotes exactly as printed. If the scan is illegible, return nulls
  rather than a guess.

Return ONLY this JSON object:

{
  "site_address": "<subject property address as printed, or null>",
  "address_quote": "<verbatim line the address came from, or null>",
  "architect":          {"firm": <string or null>, "person": <string or null>, "role_label": <exact wording or null>, "quote": <verbatim or null>},
  "civil_engineer":     {"firm": <string or null>, "person": <string or null>, "role_label": <exact wording or null>, "quote": <verbatim or null>},
  "general_contractor": {"firm": <string or null>, "person": <string or null>, "role_label": <exact wording or null>, "quote": <verbatim or null>}
}"""


def scanned_files():
    m = json.loads(Path("data/ri_zbr_matches.json").read_text())
    log = json.loads(Path("data/ri_fetch_log.json").read_text())
    out = {}
    for v in m.values():
        for u in v["urls"]:
            f = (log.get(u) or {}).get("file")
            if not f:
                continue
            p = PLANSETS / f
            if not p.exists() or f in out:
                continue
            try:
                d = fitz.open(p)
                n = d.page_count
                t = "".join((d.load_page(i).get_text() or "")
                            for i in range(min(6, n)))
                d.close()
            except Exception:
                continue
            if len(t.strip()) < 300:
                out[f] = p
    return sorted(out.values())


def page_images(pdf):
    try:
        d = fitz.open(pdf)
    except Exception:
        return []
    imgs = []
    for i in range(min(MAX_PAGES, d.page_count)):
        try:
            pix = d.load_page(i).get_pixmap(dpi=DPI)
            data = pix.tobytes("jpeg", jpg_quality=70)
        except Exception:
            continue
        if len(data) > 4_500_000:
            continue
        imgs.append(base64.standard_b64encode(data).decode())
    d.close()
    return imgs


def main():
    files = scanned_files()
    print(f"{len(files)} image-only documents")
    out, idmap, pages = [], {}, 0
    for i, f in enumerate(files):
        imgs = page_images(f)
        if not imgs:
            continue
        pages += len(imgs)
        cid = f"rs{i:04d}"
        idmap[cid] = f.name
        content = [{"type": "text", "text": f"FILE: {f.name}"}]
        for b64 in imgs:
            content.append({"type": "image",
                            "source": {"type": "base64",
                                       "media_type": "image/jpeg",
                                       "data": b64}})
        out.append({
            "custom_id": cid,
            "params": {"model": MODEL, "max_tokens": 1000, "system": SYSTEM,
                       "messages": [{"role": "user", "content": content}]},
        })
    with OUT.open("w", encoding="utf-8") as fh:
        for o in out:
            fh.write(json.dumps(o) + "\n")
    IDMAP.write_text(json.dumps(idmap, indent=1))
    tin = pages * 1400 + len(SYSTEM) / 3.6 * len(out)
    tout = 280 * len(out)
    live = tin / 1e6 * 1.00 + tout / 1e6 * 5.00
    print(f"requests {len(out)}   pages rendered {pages}")
    print(f"est input {tin/1e6:.2f}M  output {tout/1e6:.2f}M")
    print(f"COST live ${live:.2f}   batch ${live/2:.2f}")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
