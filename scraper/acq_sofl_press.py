r"""Firms named by trade press, in articles about the specific deal.

THE FIRST SOURCE IN THE BRIEF'S ORDER, and the only one that can be accepted on
its own. A Sunbiz record says who signs for an entity; an article about THIS
transaction says who bought the building. When they disagree, the article is
reporting the deal and the register is reporting the paperwork.

Each entry is keyed by the entity EXACTLY as the deed names it, and carries:

    firm    the operating company, as the publication names it
    url     the article, so every resolution can be opened and checked
    outlet  which publication
    quote   what the article actually says, so a later reader can see whether
            the firm was named as the party to THIS deal or merely mentioned

Only the outlets the brief names are used -- The Real Deal, Bisnow, Commercial
Observer -- plus Florida YIMBY, which the project already accepts. Where those
disagree with an aggregator, they win; where only an aggregator carries a deal,
it is not recorded here at all.

A JOINT VENTURE IS WRITTEN WITH SLASHES, "Pebb Capital / Wharton Properties /
Lane Capital Partners", which is the convention the Boston rows already use for
the same thing. It keeps all the partners rather than picking the one that
sounds largest.
"""

# entity as written on the deed -> resolution
PRESS = {
    # ── 545 Wyn, Wynwood. $181,000,000, 12 January 2026 ──────────────────
    "545 WYN OWNER LLC & C/O GOLDMAN PROPERTY MGMT LLC": {
        "firm": "Ken Griffin / Goldman Properties",
        "url": "https://therealdeal.com/miami/2026/01/14/ken-griffin-goldman-buy-545wyn-wynwood-office-building/",
        "outlet": "The Real Deal",
        "quote": "Ken Griffin and Goldman Properties purchased an office "
                 "property at 545 Northwest 26th Street for $181 million.",
    },
    "545 WYN LLC": {
        "firm": "Sterling Bay",
        "url": "https://therealdeal.com/miami/2026/01/14/ken-griffin-goldman-buy-545wyn-wynwood-office-building/",
        "outlet": "The Real Deal",
        "quote": "The seller was Chicago-based Sterling Bay, which built the "
                 "10-story building, known as 545Wyn, in 2021.",
    },

    # ── One Downtown, 1 SE 3rd Avenue. $110,000,000, 25 February 2026 ─────
    "1 SE 3RD AVENUE REALTY LLC": {
        "firm": "Moishe Mana",
        "url": "https://commercialobserver.com/2026/03/moishe-mana-one-downtown-miami-pccp/",
        "outlet": "Commercial Observer",
        "quote": "Moishe Mana closed on the sale of the 31-story, "
                 "450,000-square-foot One Downtown office building at 1 "
                 "Southeast Third Avenue for $110 million.",
    },
    "STIC HOLDINGS LLC & C/O PCCP LLC": {
        "firm": "PCCP / CP Group",
        "url": "https://therealdeal.com/miami/2026/03/04/pccp-cp-group-sell-one-downtown-to-moishe-mana-for-110m/",
        "outlet": "The Real Deal",
        "quote": "PCCP, based in Los Angeles, and CP Group, based in Boca "
                 "Raton, were the sellers.",
    },

    # ── Portofino Place, West Palm Beach. $208M over two deeds, 10 Aug 2026
    "FAIRFIELD PORTOFINO PLACE I LLC": {
        "firm": "Fairfield Residential",
        "url": "https://www.bisnow.com/news/south-florida/deal-sheet/cortland-fairfield-residential-portofino-place",
        "outlet": "Bisnow",
        "quote": "California-based Fairfield Residential paid $208M for the "
                 "812-unit Portofino Place East and West apartment complexes "
                 "in West Palm Beach.",
    },
    "FAIRFIELD PORTOFINO PLACE II LLC": {
        "firm": "Fairfield Residential",
        "url": "https://www.bisnow.com/news/south-florida/deal-sheet/cortland-fairfield-residential-portofino-place",
        "outlet": "Bisnow",
        "quote": "California-based Fairfield Residential paid $208M for the "
                 "812-unit Portofino Place East and West apartment complexes.",
    },
    "WPB MULTIFAMILY OWNER I LLC": {
        "firm": "Cortland",
        "url": "https://www.bisnow.com/news/south-florida/deal-sheet/cortland-fairfield-residential-portofino-place",
        "outlet": "Bisnow",
        "quote": "Cortland Partners sold the 812 apartments at 4400 and 4600 "
                 "Portofino Way to Fairfield Residential.",
    },
    "WPB MULTIFAMILY OWNER II LLC": {
        "firm": "Cortland",
        "url": "https://www.bisnow.com/news/south-florida/deal-sheet/cortland-fairfield-residential-portofino-place",
        "outlet": "Bisnow",
        "quote": "Cortland Partners sold the 812 apartments at 4400 and 4600 "
                 "Portofino Way to Fairfield Residential.",
    },

    # ── The Ponce, Coral Gables. $97,800,000, 18 June 2026 ────────────────
    "PONCE CORAL GABLES LLC": {
        "firm": "Intalex Capital / Itero Investments / Greenwall Capital",
        "url": "https://commercialobserver.com/2026/07/pgim-coral-gables-ponce/",
        "outlet": "Commercial Observer",
        "quote": "Intalex Capital, Itero Investments and Greenwall Capital "
                 "were the buyers.",
    },
    "PRISA PONCE DE LEON LLC": {
        "firm": "PGIM",
        "url": "https://therealdeal.com/miami/2026/07/06/pgim-sells-coral-gables-office-building-for-98-million/",
        "outlet": "The Real Deal",
        "quote": "PGIM sold the Ponce office complex in Coral Gables for "
                 "$97.8 million.",
    },

    # ── Miami Worldcenter Block C. $88,750,000, 13 March 2026 ─────────────
    "KASUMIGASEKI FLORIDA LLC & C/O KASUMIGASEKI CAPITAL CO LTD": {
        "firm": "Kasumigaseki Capital",
        "url": "https://www.bisnow.com/south-florida/news/land/miami-worldcenter-development-site-hits-the-market-135209",
        "outlet": "Bisnow",
        "quote": "Tokyo-based Kasumigaseki Capital closed on a 1.4-acre "
                 "parcel at Miami Worldcenter, acquiring the Block C site for "
                 "$88.75 million.",
    },

    # ── Design 41, Design District. $72,500,000, 21 January 2026 ──────────
    "41ST MIAMI DD PROPCO LLC": {
        "firm": "Wharton Properties / Pebb Capital / Lane Capital Partners",
        "url": "https://therealdeal.com/miami/2026/01/27/pebb-jeff-sutton-lane-capital-buy-design-district-building/",
        "outlet": "The Real Deal",
        "quote": "A joint venture including Jeff Sutton's Wharton Properties, "
                 "Pebb Capital, and Lane Capital Partners acquired a fully "
                 "leased mixed-use building for $72.5 million.",
    },
    "DESIGN DISTRICT DEV PARTNERS LLC": {
        "firm": "Design District Development Partners",
        "url": "https://commercialobserver.com/2026/01/miami-design-district-jeff-sutton-wharton/",
        "outlet": "Commercial Observer",
        "quote": "The seller of the building was Design District Development "
                 "Partners, which is managed by Benjamin Norton.",
    },

    # ── Mayfair House Hotel & Garden, Coconut Grove. $69,400,000, 26 May ──
    # TWO FIGURES, BOTH SOURCED, MEASURING DIFFERENT THINGS. The deed records
    # $69,400,000 and that is what `price` holds, because `price` is the
    # recorded consideration. Commercial Observer reports the hotel trading
    # "for roughly $110 million, according to a representative for Brookfield",
    # and the same article says the deed figure "likely only included the value
    # of the real estate and excluded the property's furniture and operating
    # assets". So the larger number is the going concern -- real estate plus
    # FF&E plus the operating business -- attributed to the seller's own
    # spokesman, not an independent valuation. Neither contradicts the other
    # and the attribution is named rather than left as "the press reports".
    "DEAN MCG PROPCO LLC": {
        "firm": "Elliott Investment Management / Lifestyle Hospitality Capital",
        "url": "https://commercialobserver.com/2026/06/paul-singers-elliott-management-miami-coconut-grove-mayfair-house-hotel-garden/",
        "outlet": "Commercial Observer",
        "quote": "The 179-key hotel at 3000 Florida Avenue was sold by "
                 "Brookfield Asset Management to entities affiliated with "
                 "Paul Singer's Elliott Investment Management, in partnership "
                 "with Lifestyle Hospitality Capital Group.",
    },
    "BSREP III COCONUT GROVE LLC & C/O LISA STRAUSS": {
        "firm": "Brookfield",
        "url": "https://therealdeal.com/miami/2026/06/12/brookfield-sells-mayfair-house-in-miami-for-69-million/",
        "outlet": "The Real Deal",
        "quote": "Brookfield Asset Management sold the 180-room hotel to "
                 "Elliott Investment Management.",
    },

    # ── 3480 Main Highway, Coconut Grove. $61,000,000, 30 January 2026 ────
    "ATLANTIC GATEWAY LLC": {
        "firm": "Azora",
        "url": "https://commercialobserver.com/2026/02/torose-sabal-azora-3480-main-highway-miami-coconut-grove/",
        "outlet": "Commercial Observer",
        "quote": "Azora Private purchased the seven-story property at 3480 "
                 "Main Highway.",
    },
    "3480 MAIN HIGHWAY LLC": {
        "firm": "Torose Equities / Sabal Investment Holdings",
        "url": "https://therealdeal.com/miami/2026/02/04/torose-equities-sabal-investment-flip-coconut-grove-offices/",
        "outlet": "The Real Deal",
        "quote": "Torose Equities and Sabal Investment Holdings sold a "
                 "boutique office building in Coconut Grove for $61 million.",
    },

    # ── 2401 NW 6th Avenue, Wynwood. $54,000,000, 19 March 2026 ───────────
    "GK REAL ESTATE 5 LLC": {
        "firm": "GK Real Estate",
        "url": "https://commercialobserver.com/2026/03/miami-wynwood-gary-krat-ultimate-shoes-david-sedaghati-gary-krat/",
        "outlet": "Commercial Observer",
        "quote": "Gary Krat's GK Real Estate 5 LLC acquired a Wynwood "
                 "development site at 2401 Northwest 6th Avenue for $54 "
                 "million.",
    },
    "ULTIMATE EQUITY INC": {
        "firm": "Ultimate Equity",
        "url": "https://commercialobserver.com/2026/03/miami-wynwood-gary-krat-ultimate-shoes-david-sedaghati-gary-krat/",
        "outlet": "Commercial Observer",
        "quote": "The seller was Ultimate Equity, managed by David Sedaghati "
                 "of Miami-based Ultimate Shoes.",
    },

    # ── 3265 Virginia Street, Coconut Grove. $45,500,000, 12 May 2026 ─────
    "EL AD 3265 VIRGINIA LLC & C/O EL AD NATIONAL PROPERTIES LLC": {
        "firm": "El-Ad National Properties",
        "url": "https://therealdeal.com/miami/2026/05/15/canero-flips-coconut-grove-condo-to-el-ad-national/",
        "outlet": "The Real Deal",
        "quote": "El-Ad 3265 Virginia LLC acquired the property from WNG "
                 "Miami LLC for $45.5 million.",
    },
    "WNG MIAMI LLC": {
        "firm": "Canero",
        "url": "https://commercialobserver.com/2026/05/chateau-grove-condominium-el-ad-national-canero-miami-coconut-grove/",
        "outlet": "Commercial Observer",
        "quote": "Canero flips condo in Miami's Coconut Grove to El-Ad for "
                 "$46M.",
    },
    # ── 614/720 NE 27th Street, Edgewater. $43,131,000, 21 April 2026 ────
    "720 EDGEWATER OWNER LLC": {
        "firm": "Kushner / Cain International",
        "url": "https://therealdeal.com/miami/2026/06/18/south-florida-top-real-estate-deals-june-17-2026/",
        "outlet": "The Real Deal",
        "quote": "London-based Cain and Kushner acquired an apartment "
                 "development site at 614 and 720 Northeast 27th Street in "
                 "Edgewater for $43.1 million.",
    },
    "720 NE STREET PROPCO LLC": {
        "firm": "OKO Group / Cain International",
        "url": "https://therealdeal.com/miami/2026/06/18/south-florida-top-real-estate-deals-june-17-2026/",
        "outlet": "The Real Deal",
        "quote": "...from a joint venture between Miami-based OKO Group and "
                 "Cain.",
    },

    # ── 404 Washington Avenue, Miami Beach. $31,500,000, 30 July 2026 ────
    # The Trump Group here is Eddie and Jules Trump, no relation to the
    # former president -- a distinction the deed's "% THE TRUMP GROUP" does
    # not draw and a reader would otherwise draw wrongly.
    "RJR ML ON 5TH LLC & C/O RIVANI": {
        "firm": "Rivani",
        "url": "https://therealdeal.com/miami/2026/08/04/robert-rivani-buys-south-beach-office-building/",
        "outlet": "The Real Deal",
        "quote": "Rivani, a Miami Beach-based real estate investment and "
                 "development company, acquired the mixed-use office and "
                 "retail building at 404 Washington Ave. for $31.5 million.",
    },
    "DOMINION PARTNERS LP & % THE TRUMP GROUP": {
        "firm": "The Trump Group",
        "url": "https://therealdeal.com/miami/2026/08/04/robert-rivani-buys-south-beach-office-building/",
        "outlet": "The Real Deal",
        "quote": "Dominion Partners LP, an entity affiliated with The Trump "
                 "Group led by brothers Eddie and Jules Trump, has owned the "
                 "property since 1999.",
    },

    # ── Lafayette Plaza and Emerald Terrace. 8 April 2026 ────────────────
    # SELLER ONLY. Commercial Observer reports Starwood buying both at these
    # exact prices, but dates the sales to the week of 13 August while the
    # deeds are recorded 8 April, and both buying entities are named "GH",
    # which reads as Gatehouse rather than Starwood. One source, a four-month
    # date gap and a contradicting entity name is not enough to name a buyer,
    # so the buyer stays an entity of record. The seller is not in doubt: the
    # deed's own c/o says Gatehouse and the article says Gatehouse sold.
    "LAFAYETTE SQUARE II LTD & C/O GATEHOUSE MANAGEMENT": {
        "firm": "Gatehouse Group",
        "url": "https://commercialobserver.com/2026/08/barry-sternlicht-starwood-miami-dade-affordable-housing-lafayette-emerald-terrace/",
        "outlet": "Commercial Observer",
        "quote": "Mansfield, Mass.-based Gatehouse Group developed and sold "
                 "these four properties.",
    },
    "EMERALD TERRACE LTD PARTNERSHIP & C/O GAREHOUSE GROUP INC": {
        "firm": "Gatehouse Group",
        "url": "https://commercialobserver.com/2026/08/barry-sternlicht-starwood-miami-dade-affordable-housing-lafayette-emerald-terrace/",
        "outlet": "Commercial Observer",
        "quote": "Mansfield, Mass.-based Gatehouse Group developed and sold "
                 "these four properties.",
    },

    # ── Miami Worldcenter Block E. $26,581,900, 14 May 2026 ──────────────
    "MWC BLOCK E LLC": {
        "firm": "Flow",
        "url": "https://therealdeal.com/miami/2026/06/03/flow-affiliate-sells-miami-worldcenter-building/",
        "outlet": "The Real Deal",
        "quote": "An affiliate of Adam Neumann's Flow sold an unfinished "
                 "three-story retail and office building at Miami Worldcenter "
                 "for $26.6 million.",
    },

    # ── Wynwood Jungle, 43-75 NW 23rd Street. $25,732,500, 11 June 2026 ──
    "T WYNWOOD JUNGLE NRTP FL LLC & T WYNWOOD JUNGLE NRTW FL LLC": {
        "firm": "Tabani Group",
        "url": "https://therealdeal.com/miami/2026/07/09/tabani-group-is-the-buyer-of-rivanis-wynwood-jungle/",
        "outlet": "The Real Deal",
        "quote": "Tabani Group, a Dallas-based investment firm, is the buyer "
                 "that dropped $25.7 million for the Wynwood Jungle.",
    },
    "RJR ML LLC & RJR REALTY LLC": {
        "firm": "Rivani",
        "url": "https://commercialobserver.com/2026/07/rivani-wynwood-jungle-miami-tabani/",
        "outlet": "Commercial Observer",
        "quote": "Robert Rivani has sold the Wynwood Jungle retail building "
                 "in Miami for $25.7 million.",
    },
    # ── Normandy Plaza Hotel, 6979 Collins. $26,000,000, 13 January 2026 ─
    "KT COLLINS AVE LLC": {
        "firm": "Kolter Group / BH Group",
        "url": "https://therealdeal.com/miami/2026/01/22/kolter-pays-26-million-for-condemned-miami-beach-hotel/",
        "outlet": "The Real Deal",
        "quote": "An affiliate of Delray Beach-based Kolter, led by Bobby "
                 "Julien, acquired the 90-year-old Normandy Plaza Hotel at "
                 "6979 Collins Avenue... Kolter and joint venture partner BH "
                 "Group plan a 17-story condo building.",
    },
    "VILARINO PROPERTY GROUP INC": {
        "firm": "Vilarino Property Group",
        "url": "https://therealdeal.com/miami/2026/01/22/kolter-pays-26-million-for-condemned-miami-beach-hotel/",
        "outlet": "The Real Deal",
        "quote": "Vilarino Property Group, managed by Antonio Vilarino in "
                 "Hollywood, sold the 63-room Normandy Plaza to Kolter.",
    },

    # ── Miami Worldcenter Block C, seller side. 13 March 2026 ────────────
    "MIAMI A/I PARCEL 3 SUBSIDIARY LLC": {
        "firm": "Marc Roberts",
        "url": "https://www.bisnow.com/south-florida/news/deal-sheet/japanese-developer-first-us-purchase-with-89m-miami-worldcenter-site-purchase-south-florida-deal-sheet-133786",
        "outlet": "Bisnow",
        "quote": "Kasumigaseki Capital paid Miami A/I Parcel 3 Subsidiary, an "
                 "entity led by E11even Miami co-founder Marc Roberts, $88.8M "
                 "for a development site at Miami Worldcenter.",
    },
    # ── 2125 Biscayne Boulevard, Edgewater. $19,000,000, 26 March 2026 ──
    "VALORO BISCAYNE OWNER LLC": {
        "firm": "Valoro Capital",
        "url": "https://therealdeal.com/miami/2026/04/16/south-florida-top-real-estate-deals-wednesday-april-15-2026/",
        "outlet": "The Real Deal",
        "quote": "Valoro Capital acquired the office building in Miami's "
                 "Edgewater neighborhood for $19 million after Novel "
                 "Coworking sold the building.",
    },
    "2125 BISCAYNE NOVEL COWORKING LLC": {
        "firm": "Novel Coworking",
        "url": "https://therealdeal.com/miami/2026/04/16/south-florida-top-real-estate-deals-wednesday-april-15-2026/",
        "outlet": "The Real Deal",
        "quote": "...after Novel Coworking sold the building following their "
                 "purchase of the property for $20.6 million in 2019.",
    },

    # ── Woma, Wynwood Norte. $25,000,000, 5 June 2026 ────────────────────
    # BUYER ONLY. The press names Kambucha LLC's manager, Rosa Lewkowicz, but
    # no operating company behind her, and a manager's name is not a firm. The
    # seller therefore stays an entity of record.
    "WOMA WYNWOOD LLC": {
        "firm": "Uribe Schwarzkopf",
        "url": "https://therealdeal.com/miami/2026/09/01/uribe-schwarzkopf-launches-first-u-s-condo-project-in-miami/",
        "outlet": "The Real Deal",
        "quote": "Woma marks the Ecuador-based developer Uribe Schwarzkopf's "
                 "first project in the United States, on the site at 444 "
                 "Northwest 33rd Street bought for $25 million.",
    },
    # ── Delta Hotels by Marriott, 1301 Belvedere Rd. $23,750,000 ────────
    # BUYER ONLY. Palm Beach County publishes no grantor for this parcel --
    # its 2026 sale is the first in its recorded history, so there is no prior
    # owner to derive one from. The press names Activate Hospitality as the
    # seller, but with no entity on the deed there is nothing to attach that
    # to; the row's seller stays genuinely empty rather than being filled from
    # a source the record cannot be reconciled against.
    "PM FLORIDA MASTER LP": {
        "firm": "Palm Holdings",
        "url": "https://therealdeal.com/miami/2026/01/30/activate-hospitality-sells-renovated-west-palm-beach-hotel/",
        "outlet": "The Real Deal",
        "quote": "Activate Hospitality sold the 11-story, 200-key Delta "
                 "Hotels by Marriott West Palm Beach to an affiliate of "
                 "Toronto-based Palm Holdings, led by Sheetal Kapoor.",
    },
    # ── 320-380 NW 26th Street, Wynwood. $24,481,200, 5 July 2026 ───────
    # BUYER ONLY. The Real Deal describes the seller as "a New York company
    # tied to David Edelstein", but the deed names a revocable trust in
    # entirely different personal names. One of those is wrong and the record
    # cannot say which, so the seller stays an entity of record.
    "320 NW 26 STREET REALTY LLC": {
        "firm": "Moishe Mana",
        "url": "https://therealdeal.com/miami/2026/07/21/south-florida-top-real-estate-deals-july-20-2026/",
        "outlet": "The Real Deal",
        "quote": "The buildings at 380 and 320 Northwest 26th Street in "
                 "Wynwood were sold for $24.5 million... the buyer was a "
                 "company linked to developer and investor Moishe Mana.",
    },
    # ── Lafayette Plaza and Emerald Terrace, buyer side. 8 April 2026 ───
    # RESOLVED ON RE-EXAMINATION. Two objections stood against naming Starwood
    # and both have now failed.
    #
    # THE DATE. The deeds state 8 April while Commercial Observer dates the
    # sales to mid-August. Miami-Dade book numbers rise monotonically with
    # RECORDING, and these are book 35443 -- between a deed recorded 12 August
    # and one recorded 13 August elsewhere in this same dataset. So 8 April is
    # the execution date and the instrument reached the register in August,
    # exactly when the article says. The gap was never a contradiction.
    #
    # THE NAME. "GH" reads as Gatehouse, which is the seller. The Florida
    # register says otherwise: GH LAFAYETTE PLAZA, L.P. and GH EMERALD
    # TERRACE, L.P. are foreign limited partnerships filed the same day,
    # 20 November 2025, both with their principal address at 591 West Putnam
    # Avenue, Greenwich, Connecticut -- Starwood Capital Group's headquarters
    # -- and each with a general partner, GH <NAME> GP, L.L.C., at that same
    # address. A seller does not register the buying vehicle at the buyer's
    # head office.
    "GH LAFAYETTE PLAZA L P": {
        "firm": "Starwood Asset Management",
        "url": "https://commercialobserver.com/2026/08/barry-sternlicht-starwood-miami-dade-affordable-housing-lafayette-emerald-terrace/",
        "outlet": "Commercial Observer",
        "quote": "Barry Sternlicht's Starwood Asset Management bought the "
                 "Lafayette housing complex in Miami's Little River "
                 "neighborhood for $27 million from the Gatehouse Group; the "
                 "Florida register puts the buying partnership's principal "
                 "address at Starwood Capital's Greenwich headquarters.",
    },
    "GH EMERALD TERRACE L P": {
        "firm": "Starwood Asset Management",
        "url": "https://commercialobserver.com/2026/08/barry-sternlicht-starwood-miami-dade-affordable-housing-lafayette-emerald-terrace/",
        "outlet": "Commercial Observer",
        "quote": "Starwood Asset Management bought Emerald Terrace for $24.6 "
                 "million from the Gatehouse Group; the Florida register puts "
                 "the buying partnership at 591 West Putnam Avenue, "
                 "Greenwich, Starwood Capital's headquarters.",
    },
}


def lookup(entity):
    """The press resolution for an entity named on a deed, or None."""
    return PRESS.get(str(entity or "").strip().upper())
