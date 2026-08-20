"""Address and firm-name normalisation shared by every matcher in this run."""
import re

NUMBER_WORDS = {
    "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
    "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
    "eleven": "11", "twelve": "12", "fifteen": "15", "twenty": "20",
}

# Spelled-out forms that appear in municipal filings but not in the tracker.
WORD_FORM = {
    "mount": "mt", "saint": "st", "fort": "ft", "doctor": "dr",
    "mt.": "mt", "st.": "st",
}

SUFFIX = {
    "street": "st", "str": "st", "st": "st",
    "avenue": "ave", "av": "ave", "ave": "ave",
    "boulevard": "blvd", "blvd": "blvd",
    "road": "rd", "rd": "rd",
    "drive": "dr", "dr": "dr",
    "place": "pl", "pl": "pl",
    "square": "sq", "sq": "sq",
    "court": "ct", "ct": "ct",
    "lane": "ln", "ln": "ln",
    "terrace": "ter", "terr": "ter", "ter": "ter",
    "parkway": "pkwy", "pkwy": "pkwy",
    "highway": "hwy", "hwy": "hwy",
    "circle": "cir", "cir": "cir",
    "wharf": "wharf", "way": "way", "row": "row", "mall": "mall",
    "park": "park", "path": "path", "pier": "pier", "alley": "aly",
}

DIRECTIONAL = {
    "north": "n", "south": "s", "east": "e", "west": "w",
    "northeast": "ne", "northwest": "nw", "southeast": "se", "southwest": "sw",
}

UNIT_RE = re.compile(
    r"\b(unit|suite|ste|apt|apartment|floor|fl|building|bldg|#)\s*[\w\-]*", re.I)
PAREN_RE = re.compile(r"\([^)]*\)")


def norm_address(a):
    """Return a canonical address string, or '' when there is nothing to match."""
    if not a:
        return ""
    s = str(a).lower().strip()
    s = PAREN_RE.sub(" ", s)
    s = s.split(",")[0]                      # drop city/state/zip tail
    s = UNIT_RE.sub(" ", s)
    s = re.sub(r"[^\w\s\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = []
    parts = s.split()
    for i, t in enumerate(parts):
        if i == 0 and t in NUMBER_WORDS:
            t = NUMBER_WORDS[t]
        if t in DIRECTIONAL:
            t = DIRECTIONAL[t]
        # "Mount Pleasant Ave" and "Mt Pleasant Ave" are the same street, but
        # only when the word leads a name -- never collapse a trailing suffix.
        if t in WORD_FORM and i < len(parts) - 1:
            t = WORD_FORM[t]
        if t in SUFFIX:
            t = SUFFIX[t]
        toks.append(t)
    return " ".join(toks).strip()


def address_keys(a):
    """Every (number, street) pair a free-text address could refer to.

    Municipal filings caption several lots at once. Two shapes matter:
    "157, 159 & 165 GANO STREET", where bare numbers share the street that
    follows them, and "116 Waterman Street & 232 Brook Street", where each
    number belongs to its own street. Splitting on the conjunction and
    carrying unattached numbers forward handles both without inventing an
    address that was never written.
    """
    if not a:
        return set()
    head = re.split(
        r"\b(?:providence|cranston|warwick|pawtucket|newport|rhode\s+island)\b",
        str(a), flags=re.I)[0]
    segments = re.split(r"\s*(?:&|\band\b|/)\s*", head, flags=re.I)

    keys, pending = set(), []
    for seg in segments:
        nums = [int(n) for n in re.findall(r"\d{1,5}", seg)]
        if not nums:
            pending = []
            continue
        last = str(nums[-1])
        tail = seg[seg.rfind(last) + len(last):]
        tail = re.sub(r"^[\s,\-]+", "", tail)
        sn = street_name("1 " + tail) if tail.strip() else ""
        if not sn:
            pending.extend(nums)      # bare numbers waiting for their street
            continue
        for n in nums + pending:
            keys.add((n, sn))
        pending = []
    return keys


def street_numbers(a):
    """Every street number an address covers. '100-114 Hampden' -> {100,114}.

    Ranges are kept as endpoints only; a filing and a permit rarely agree on
    the interior numbers, and expanding the range invites false positives.
    """
    if not a:
        return set()
    head = str(a).strip().split(",")[0]
    m = re.match(r"\s*(\d+)\s*(?:[-\u2013]\s*(\d+))?", head)
    if not m:
        w = head.lower().split()
        if w and w[0] in NUMBER_WORDS:
            return {int(NUMBER_WORDS[w[0]])}
        return set()
    out = {int(m.group(1))}
    if m.group(2):
        out.add(int(m.group(2)))
    return out


def street_name(a):
    """The address with its leading number(s) removed."""
    n = norm_address(a)
    return re.sub(r"^[\d\s\-]+", "", n).strip()


FIRM_NOISE = re.compile(
    r"\b(inc|llc|llp|lp|ltd|co|corp|corporation|company|incorporated|"
    r"pc|pa|plc|associates|assoc|group|partners|partnership|the)\b\.?", re.I)


def norm_firm(name):
    if not name:
        return ""
    s = str(name).lower()
    s = re.sub(r"&", " and ", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = FIRM_NOISE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()
