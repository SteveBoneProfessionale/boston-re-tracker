r"""
Read the architect off a municipal PDF found by web search.

The web pass keeps landing on the same kind of page: a city agenda that links
the applicant's own plan set or narrative. That document names the architect in
its title block, which is stronger evidence than a news story paraphrasing it,
so it is worth opening rather than skipping.

WebFetch cannot read a PDF -- it hands back the raw stream -- so this fetches
and extracts locally, then prints only the lines mentioning an architect. It
prints; it never writes. Judging what the line means, and whether it belongs to
this project, stays a human/model decision, and the write still goes through
ri_arch_web.py and its guards.

    python scraper/ri_arch_pdf_probe.py <url> [<url> ...]
"""

import io
import re
import sys
import urllib.request

import pymupdf

CACHE = {}
# A title block does not always use the word. 870 Westminster's drawing set
# named its author only as "mcgeorgeai.com" over a street address -- the sheet
# never says "architect" once. So a web/e-mail address counts as a hit too:
# on a stamped drawing the domain in the title block IS the firm.
HIT = re.compile("architect|[a-z0-9.-]+[.](?:com|net|org)|(?:^| )A[.]?I[.]?A(?: |$)", re.I)
UA = {"User-Agent": "Mozilla/5.0 (compatible; ri-pipeline-research/1.0)"}


def probe(url, max_pages=6, width=90):
    try:
        req = urllib.request.Request(url, headers=UA)
        raw = urllib.request.urlopen(req, timeout=60).read()
    except Exception as e:
        print("  FETCH FAILED %s -- %s" % (url, e))
        return
    try:
        doc = pymupdf.open(stream=io.BytesIO(raw), filetype="pdf")
    except Exception as e:
        print("  NOT A PDF %s -- %s" % (url, e))
        return
    print("== %s  (%d pages, %.1f MB)" % (url, doc.page_count, len(raw) / 1e6))
    seen = set()
    for i in range(min(max_pages, doc.page_count)):
        for line in doc[i].get_text().splitlines():
            line = " ".join(line.split())
            if not HIT.search(line) or len(line) < 4:
                continue
            key = line.lower()
            if key in seen:
                continue
            seen.add(key)
            # The Windows console is cp1252; a bullet glyph out of a PDF
            # font would otherwise kill the run mid-document.
            safe = line[:width].encode("ascii", "replace").decode("ascii")
            print("  p%-2d %s" % (i + 1, safe))
    if not seen:
        print("  (no line mentions an architect in the first %d pages)" % max_pages)
    doc.close()


if __name__ == "__main__":
    for u in sys.argv[1:]:
        probe(u)
