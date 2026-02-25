"""
Mess injection functions for payor data generator.
Each function takes clean data and returns a messy variant.
"""

import random
from faker import Faker

fake = Faker()
Faker.seed(42)

# ---------------------------------------------------------------------------
# Name variants
# ---------------------------------------------------------------------------

LEGAL_SUFFIXES = ["Inc.", "Inc", "LLC", "Corp.", "Corporation", "Co.", "L.P.", "Ltd."]
BCBS_VARIANTS = [
    "Blue Cross Blue Shield",
    "BlueCross BlueShield",
    "BCBS",
    "Blue Cross / Blue Shield",
    "Blue Cross and Blue Shield",
]


def name_variant(name: str, source: str) -> str:
    """Apply source-specific name mess."""
    r = random.random()

    # CRM: cleanest, but sometimes has legal suffix
    if source == "crm":
        if r < 0.3:
            suffix = random.choice(LEGAL_SUFFIXES)
            return f"{name}, {suffix}" if r < 0.15 else f"{name} {suffix}"
        return name

    # Claims: UPPER, abbreviations, stripped
    if source == "claims":
        n = name.upper()
        if r < 0.4:
            n = n.replace(",", "").replace(".", "")
        if r < 0.3 and "BLUE CROSS" in n.upper():
            variant = random.choice(BCBS_VARIANTS).upper()
            n = n.upper().replace("BLUE CROSS BLUE SHIELD", variant)
        return n

    # Credentialing: DBA names, slight variations
    if source == "credentialing":
        if r < 0.2:
            return f"{name} Health Inc."
        if r < 0.35:
            return name.replace("Healthcare", "Health Care")
        return name

    # Reference: official, sometimes with "of" variants
    if source == "cms_reference":
        if r < 0.15:
            return name.replace(", Inc.", "").replace(" Inc.", "").replace(" Inc", "")
        return name

    return name


# ---------------------------------------------------------------------------
# Address mess
# ---------------------------------------------------------------------------

STREET_ABBREVS = {
    "Street": ["St", "St.", "Str"],
    "Avenue": ["Ave", "Ave.", "Av"],
    "Boulevard": ["Blvd", "Blvd."],
    "Drive": ["Dr", "Dr."],
    "Road": ["Rd", "Rd."],
    "Suite": ["Ste", "Ste.", "STE", "#"],
}


def address_variant(addr: str) -> str:
    """Mess up an address string."""
    r = random.random()
    for full, abbrevs in STREET_ABBREVS.items():
        if full in addr and r < 0.5:
            addr = addr.replace(full, random.choice(abbrevs))
    return addr


def single_line_address(addr1: str, addr2: str | None, city: str, state: str, zipcode: str) -> str:
    """Claims system: single-line address."""
    parts = [addr1]
    if addr2:
        parts.append(addr2)
    parts.extend([city, state, zipcode])
    return ", ".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# State format
# ---------------------------------------------------------------------------

STATE_FULL = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}


def state_variant(state_code: str, source: str) -> str:
    """Claims often uses full state name or mixed case."""
    if source == "claims" and random.random() < 0.4:
        full = STATE_FULL.get(state_code, state_code)
        return random.choice([full, full.lower(), state_code.lower()])
    return state_code


# ---------------------------------------------------------------------------
# Zip code mess
# ---------------------------------------------------------------------------

def zip_variant(zipcode: str, source: str) -> str:
    """Mess up zip codes."""
    if not zipcode:
        return zipcode
    r = random.random()
    if source in ("claims", "cms_reference") and r < 0.3:
        # Add zip+4
        return f"{zipcode}-{random.randint(1000, 9999)}"
    if source == "claims" and r < 0.15:
        # Drop leading zero
        return zipcode.lstrip("0") or zipcode
    return zipcode


# ---------------------------------------------------------------------------
# Phone format
# ---------------------------------------------------------------------------

def phone_variant(phone: str | None) -> str | None:
    """Different phone formats."""
    if not phone:
        return None
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) < 10:
        return phone
    d = digits[-10:]
    formats = [
        f"{d[:3]}-{d[3:6]}-{d[6:]}",
        f"({d[:3]}) {d[3:6]}-{d[6:]}",
        d,
        f"{d[:3]}.{d[3:6]}.{d[6:]}",
        f"1-{d[:3]}-{d[3:6]}-{d[6:]}",
    ]
    return random.choice(formats)


# ---------------------------------------------------------------------------
# Missing fields
# ---------------------------------------------------------------------------

def maybe_null(value, probability: float = 0.15):
    """Randomly null out a field."""
    if random.random() < probability:
        return None
    return value


# ---------------------------------------------------------------------------
# Tax ID mess
# ---------------------------------------------------------------------------

def tax_id_variant(tax_id: str | None, source: str) -> str | None:
    """Format tax ID differently or inject typos."""
    if not tax_id:
        return None
    digits = "".join(c for c in tax_id if c.isdigit())
    r = random.random()
    if source == "crm":
        # CRM usually has dashes
        return f"{digits[:2]}-{digits[2:]}" if len(digits) >= 3 else digits
    if source == "claims" and r < 0.1:
        # Typo: swap two digits
        d = list(digits)
        if len(d) >= 5:
            i = random.randint(2, len(d) - 2)
            d[i], d[i + 1] = d[i + 1], d[i]
        return "".join(d)
    return digits


# ---------------------------------------------------------------------------
# Stale data
# ---------------------------------------------------------------------------

def maybe_stale_status(is_active: bool, source: str) -> bool:
    """Claims sometimes shows terminated payors as active."""
    if source == "claims" and not is_active and random.random() < 0.15:
        return True  # stale: still shows active
    return is_active
