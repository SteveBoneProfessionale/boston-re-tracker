"""Keyword-based topic classifier for news articles."""

TOPICS = [
    "Events",
    "Architecture",
    "Construction",
    "Development",
    "Engineering",
    "Financing",
    "Investments",
    "Leasing",
    "Retail",
]

_RULES: dict[str, list[str]] = {
    "Events": [
        "event", "conference", "summit", "forum", "award", "ceremony",
        "gala", "seminar", "webinar", "panel discussion", "ribbon cutting",
        "ribbon-cutting", "open house", "networking", "celebration",
        "inaugural", "annual meeting", "expo", "trade show", "spotlight",
    ],
    "Architecture": [
        "architect", "architecture", " design", "facade", "rendering",
        "blueprint", "leed", "sustainable design", "historic preservation",
        "adaptive reuse", "landmark", "skyline", "floor plan",
        "mixed-use design", "building design", "exterior", "architectural",
    ],
    "Construction": [
        "construction", "groundbreaking", "ground breaking", "broke ground",
        "breaking ground", "builder", "contractor", "crane", "demolish",
        "demolition", "excavation", "foundation", "under construction",
        "completion", "topping out", "topped out", "build out", "buildout",
        "renovation", "rehab", "rehabilitation", "safest firms", "build",
    ],
    "Development": [
        "development", "developer", "bpda", "zoning", "article 80", "pnf",
        "dpir", "planning approval", "proposal", "mixed-use", "redevelopment",
        "housing project", "master plan", "site plan", "permitting",
        "entitlement", "ground lease", "planned unit", "expansion",
    ],
    "Engineering": [
        "engineer", "engineering", "mep", "structural", "civil engineer",
        "infrastructure", "utilities", "mechanical", "electrical",
        "plumbing", "geotechnical", "environmental remediation", "survey",
    ],
    "Financing": [
        "financing", "loan", "mortgage", "debt financing", "capital raise",
        "refinanc", "lender", "credit facility", "bond", "tax credit",
        "bridge loan", "construction loan", "mezzanine", "preferred equity",
        "fundrais", "closes financing", "secures financing", "hud loan",
        "fannie mae", "freddie mac", "cmbs",
    ],
    "Investments": [
        "investment", "investor", "acquisition", "acquir", "sale closes",
        "sells for", "sold for", "portfolio", "reit", "deal closes",
        "transaction", "trades hands", "joint venture", "ownership stake",
        "recapitalization", "equity stake", "purchased", "million sale",
        "million acquisition", "trade hands", "closes $", "closes deal",
    ],
    "Leasing": [
        "lease", "leasing", "tenant", "landlord", "vacancy", "occupancy",
        "square-foot", "asking rate", "sublease", "renewal", "leased",
        "signs lease", "new tenant", "anchor tenant", "preleased",
        "pre-leased", "office space", "retail space", "industrial space",
        "sq. ft", " sf ", "brokers",
    ],
    "Retail": [
        "retail", "restaurant", "dining", "shopping", "boutique",
        "food hall", "food and beverage", "entertainment venue", "hotel",
        "hospitality", "franchise", "storefront", "grocery", "supermarket",
        "fitness center", "gym", "spa", "opens first", "opens new",
    ],
}


def classify_topics(title: str, summary: str) -> str:
    """Return comma-separated topic tags for an article, or empty string if none match."""
    text = (title + " " + (summary or "")).lower()
    matched = []
    for topic in TOPICS:
        for kw in _RULES[topic]:
            if kw in text:
                matched.append(topic)
                break
    return ",".join(matched)
