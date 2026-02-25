-- ===========================================================================
-- Individual Strategy UDFs — MDM.MATCH schema
-- ---------------------------------------------------------------------------
-- One UDF per matching strategy for direct testing and debugging.
-- The main ENSEMBLE_MATCH_SCORE UDF calls these internally, but they're
-- also available standalone.
-- ===========================================================================

USE DATABASE MDM;
USE SCHEMA MATCH;

-- =========================================================================
-- NAME STRATEGIES
-- =========================================================================

-- NAME_JW: Jaro-Winkler similarity
CREATE OR REPLACE FUNCTION MDM.MATCH.NAME_JW_SCORE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('jellyfish')
HANDLER = 'score'
AS $$
import jellyfish
def score(a, b):
    if not a or not b:
        return None
    return jellyfish.jaro_winkler_similarity(a.upper().strip(), b.upper().strip())
$$;

-- NAME_TSR: Token Sort Ratio
CREATE OR REPLACE FUNCTION MDM.MATCH.NAME_TSR_SCORE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
from difflib import SequenceMatcher
def score(a, b):
    if not a or not b:
        return None
    sa = ' '.join(sorted(a.upper().split()))
    sb = ' '.join(sorted(b.upper().split()))
    return SequenceMatcher(None, sa, sb).ratio()
$$;

-- NAME_PHO: Double Metaphone phonetic comparison
CREATE OR REPLACE FUNCTION MDM.MATCH.NAME_PHO_SCORE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('metaphone')
HANDLER = 'score'
AS $$
from metaphone import doublemetaphone
def score(a, b):
    if not a or not b:
        return None
    ma = doublemetaphone(a.upper().strip())
    mb = doublemetaphone(b.upper().strip())
    if (ma[0] and mb[0] and ma[0] == mb[0]) or \
       (ma[0] and mb[1] and ma[0] == mb[1]) or \
       (ma[1] and mb[0] and ma[1] == mb[0]):
        return 1.0
    return 0.0
$$;

-- NAME_ABR: Abbreviation expansion + Jaro-Winkler
CREATE OR REPLACE FUNCTION MDM.MATCH.NAME_ABR_SCORE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('jellyfish')
HANDLER = 'score'
AS $$
import jellyfish

ABBREVS = {
    "BCBS": "BLUE CROSS BLUE SHIELD", "BC": "BLUE CROSS", "BS": "BLUE SHIELD",
    "UHC": "UNITEDHEALTHCARE", "UHG": "UNITEDHEALTH GROUP",
    "HCSC": "HEALTH CARE SERVICE CORPORATION", "KP": "KAISER PERMANENTE",
    "KFHP": "KAISER FOUNDATION HEALTH PLAN", "HMO": "HEALTH MAINTENANCE ORGANIZATION",
    "PPO": "PREFERRED PROVIDER ORGANIZATION", "EPO": "EXCLUSIVE PROVIDER ORGANIZATION",
    "POS": "POINT OF SERVICE", "MVP": "MVP HEALTH CARE", "HAP": "HEALTH ALLIANCE PLAN",
    "PHP": "PHYSICIANS HEALTH PLAN", "QHP": "QUALIFIED HEALTH PLAN",
    "FEHB": "FEDERAL EMPLOYEES HEALTH BENEFITS", "CCHP": "CHINESE COMMUNITY HEALTH PLAN",
}

def _expand(name):
    tokens = name.upper().split()
    return ' '.join(ABBREVS.get(t, t) for t in tokens)

def score(a, b):
    if not a or not b:
        return None
    return jellyfish.jaro_winkler_similarity(_expand(a), _expand(b))
$$;

-- NAME_NGR: Character trigram cosine similarity
CREATE OR REPLACE FUNCTION MDM.MATCH.NAME_NGR_SCORE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
from collections import Counter
import math

def _ngrams(s, n=3):
    s = s.upper()
    return [s[i:i+n] for i in range(len(s)-n+1)] if len(s) >= n else [s]

def score(a, b):
    if not a or not b:
        return None
    ga = Counter(_ngrams(a))
    gb = Counter(_ngrams(b))
    common = set(ga) & set(gb)
    dot = sum(ga[g] * gb[g] for g in common)
    ma = math.sqrt(sum(v*v for v in ga.values()))
    mb = math.sqrt(sum(v*v for v in gb.values()))
    return dot / (ma * mb) if ma and mb else 0.0
$$;

-- =========================================================================
-- ADDRESS STRATEGIES
-- =========================================================================

-- ADDR_JAC: Token Jaccard (stopword-removed)
CREATE OR REPLACE FUNCTION MDM.MATCH.ADDR_JAC_SCORE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
STOPS = {'ST','STREET','AVE','AVENUE','BLVD','BOULEVARD','RD','ROAD',
         'DR','DRIVE','LN','LANE','CT','COURT','STE','SUITE','APT','UNIT','FL','FLOOR','#'}
def score(a, b):
    if not a or not b:
        return None
    ta = set(t.upper().rstrip('.,') for t in a.split()) - STOPS
    tb = set(t.upper().rstrip('.,') for t in b.split()) - STOPS
    union = ta | tb
    return len(ta & tb) / len(union) if union else 0.0
$$;

-- ADDR_CMP: Component-level match (street number + street name)
CREATE OR REPLACE FUNCTION MDM.MATCH.ADDR_CMP_SCORE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('jellyfish')
HANDLER = 'score'
AS $$
import jellyfish

STOPS = {'ST','STREET','AVE','AVENUE','BLVD','BOULEVARD','RD','ROAD',
         'DR','DRIVE','LN','LANE','CT','COURT','STE','SUITE','APT','UNIT','FL','FLOOR','#'}

def _parse(addr):
    tokens = addr.upper().split()
    r = {}
    if tokens and tokens[0].replace('-','').isdigit():
        r['num'] = tokens[0]
        tokens = tokens[1:]
    r['name'] = ' '.join(t for t in tokens[:3] if t not in STOPS)
    return r

def score(a, b):
    if not a or not b:
        return None
    ca, cb = _parse(a), _parse(b)
    total_w, s = 0.0, 0.0
    for comp, w in [('num', 0.30), ('name', 0.30)]:
        va, vb = ca.get(comp, ''), cb.get(comp, '')
        if va and vb:
            total_w += w
            s += w * (1.0 if va == vb else jellyfish.jaro_winkler_similarity(va, vb))
    return s / total_w if total_w > 0 else 0.0
$$;

-- ADDR_ZIP: ZIP proximity + street overlap
CREATE OR REPLACE FUNCTION MDM.MATCH.ADDR_ZIP_SCORE(addr_a VARCHAR, addr_b VARCHAR, zip_a VARCHAR, zip_b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
STOPS = {'ST','STREET','AVE','AVENUE','BLVD','BOULEVARD','RD','ROAD',
         'DR','DRIVE','LN','LANE','CT','COURT','STE','SUITE','APT','UNIT','FL','FLOOR','#'}
def score(addr_a, addr_b, zip_a, zip_b):
    if not zip_a or not zip_b:
        return None
    zip_score = 1.0 if zip_a == zip_b else (0.5 if zip_a[:3] == zip_b[:3] else 0.0)
    if addr_a and addr_b:
        ta = set(t.upper().rstrip('.,') for t in addr_a.split()) - STOPS
        tb = set(t.upper().rstrip('.,') for t in addr_b.split()) - STOPS
        union = ta | tb
        jac = len(ta & tb) / len(union) if union else 0.0
    else:
        jac = 0.0
    return zip_score * 0.5 + jac * 0.5
$$;

-- ADDR_NRM: USPS-style normalization + Jaro-Winkler
CREATE OR REPLACE FUNCTION MDM.MATCH.ADDR_NRM_SCORE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('jellyfish')
HANDLER = 'score'
AS $$
import jellyfish

NORMS = {"STREET":"ST","AVENUE":"AVE","BOULEVARD":"BLVD","ROAD":"RD",
         "DRIVE":"DR","LANE":"LN","COURT":"CT","PLACE":"PL","CIRCLE":"CIR",
         "TERRACE":"TER","NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W",
         "NORTHEAST":"NE","NORTHWEST":"NW","SOUTHEAST":"SE","SOUTHWEST":"SW",
         "SUITE":"STE","APARTMENT":"APT"}

def _norm(addr):
    return ' '.join(NORMS.get(t, t) for t in addr.upper().split())

def score(a, b):
    if not a or not b:
        return None
    na, nb = _norm(a), _norm(b)
    return 1.0 if na == nb else jellyfish.jaro_winkler_similarity(na, nb)
$$;

-- =========================================================================
-- PHONE STRATEGY
-- =========================================================================

-- PHONE_E164: Full 10-digit normalized exact match
CREATE OR REPLACE FUNCTION MDM.MATCH.PHONE_E164_SCORE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
import re
def score(a, b):
    if not a or not b:
        return None
    da = re.sub(r'[^0-9]', '', a)
    db = re.sub(r'[^0-9]', '', b)
    if len(da) >= 10 and len(db) >= 10:
        return 1.0 if da[-10:] == db[-10:] else 0.0
    if len(da) >= 7 and len(db) >= 7:
        return 1.0 if da[-7:] == db[-7:] else 0.0
    return 0.0
$$;

-- =========================================================================
-- TAX ID STRATEGY
-- =========================================================================

-- TAXID_EXACT: Exact digit match
CREATE OR REPLACE FUNCTION MDM.MATCH.TAXID_EXACT(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
import re
def score(a, b):
    if not a or not b:
        return None
    da = re.sub(r'[^0-9]', '', a)
    db = re.sub(r'[^0-9]', '', b)
    return 1.0 if da and db and da == db else 0.0
$$;

-- TAXID_TRANSPOSE: Detect transposition/single-digit errors
CREATE OR REPLACE FUNCTION MDM.MATCH.TAXID_TRANSPOSE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
import re
def score(a, b):
    if not a or not b:
        return None
    da = re.sub(r'[^0-9]', '', a)
    db = re.sub(r'[^0-9]', '', b)
    if not da or not db:
        return None
    if da == db:
        return 1.0
    if len(da) != len(db):
        return 0.0
    diffs = sum(1 for x, y in zip(da, db) if x != y)
    if diffs <= 1:
        return 0.9
    if diffs == 2:
        return 0.7
    return 0.0
$$;

-- TAXID_PREFIX: First 2 digits match + partial remaining
CREATE OR REPLACE FUNCTION MDM.MATCH.TAXID_PREFIX(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
import re
def score(a, b):
    if not a or not b:
        return None
    da = re.sub(r'[^0-9]', '', a)
    db = re.sub(r'[^0-9]', '', b)
    if not da or not db or len(da) < 5 or len(db) < 5:
        return None
    if da[:2] != db[:2]:
        return 0.0
    matching = sum(1 for x, y in zip(da[2:], db[2:]) if x == y)
    return 0.6 if matching >= 5 else 0.3
$$;

-- =========================================================================
-- PHONE STRATEGIES (additional)
-- =========================================================================

-- PHONE_LAST7: Last 7 digits match (local number)
CREATE OR REPLACE FUNCTION MDM.MATCH.PHONE_LAST7(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
import re
def score(a, b):
    if not a or not b:
        return None
    da = re.sub(r'[^0-9]', '', a)
    db = re.sub(r'[^0-9]', '', b)
    if len(da) >= 7 and len(db) >= 7:
        return 1.0 if da[-7:] == db[-7:] else 0.0
    return 0.0
$$;

-- PHONE_AREACODE: Area code + last-4 partial match
CREATE OR REPLACE FUNCTION MDM.MATCH.PHONE_AREACODE(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
import re
def score(a, b):
    if not a or not b:
        return None
    da = re.sub(r'[^0-9]', '', a)
    db = re.sub(r'[^0-9]', '', b)
    if len(da) >= 10 and len(db) >= 10:
        ac_match = da[-10:-7] == db[-10:-7]
        last4 = da[-4:] == db[-4:]
        if ac_match and last4:
            return 0.8
        if ac_match:
            return 0.3
    return 0.0
$$;

-- =========================================================================
-- CMS STRATEGIES
-- =========================================================================

-- CMS_EXACT: Exact CMS Plan ID match
CREATE OR REPLACE FUNCTION MDM.MATCH.CMS_EXACT(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
def score(a, b):
    if not a or not b:
        return None
    return 1.0 if a.strip().upper() == b.strip().upper() else 0.0
$$;

-- CMS_PREFIX: First 5 chars of CMS Plan ID match
CREATE OR REPLACE FUNCTION MDM.MATCH.CMS_PREFIX(a VARCHAR, b VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'score'
AS $$
def score(a, b):
    if not a or not b:
        return None
    ca, cb = a.strip().upper(), b.strip().upper()
    if len(ca) >= 5 and len(cb) >= 5 and ca[:5] == cb[:5]:
        return 0.85
    return 0.0
$$;
-- ===========================================================================
-- Ensemble Match Score UDF — MDM.MATCH.ENSEMBLE_MATCH_SCORE
-- ---------------------------------------------------------------------------
-- Data-characteristic router (Option C): inspects each field pair, selects
-- 2-3 appropriate strategies, returns flat result with per-field winning
-- score/strategy and weighted composite.
-- ===========================================================================

USE DATABASE MDM;
USE SCHEMA MATCH;

CREATE OR REPLACE FUNCTION MDM.MATCH.ENSEMBLE_MATCH_SCORE(
    name_a VARCHAR, name_b VARCHAR,
    tax_a VARCHAR, tax_b VARCHAR,
    addr_a VARCHAR, addr_b VARCHAR,
    phone_a VARCHAR, phone_b VARCHAR,
    cms_a VARCHAR, cms_b VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('jellyfish', 'metaphone')
HANDLER = 'ensemble_match_score'
AS
$$
import jellyfish
from metaphone import doublemetaphone
from difflib import SequenceMatcher
from collections import Counter
import math
import re

# ── Abbreviation dictionary ──────────────────────────────────────────────

ABBREVS = {
    "BCBS": "BLUE CROSS BLUE SHIELD", "BC": "BLUE CROSS", "BS": "BLUE SHIELD",
    "UHC": "UNITEDHEALTHCARE", "UHG": "UNITEDHEALTH GROUP",
    "HCSC": "HEALTH CARE SERVICE CORPORATION", "KP": "KAISER PERMANENTE",
    "KFHP": "KAISER FOUNDATION HEALTH PLAN", "HMO": "HEALTH MAINTENANCE ORGANIZATION",
    "PPO": "PREFERRED PROVIDER ORGANIZATION", "EPO": "EXCLUSIVE PROVIDER ORGANIZATION",
    "POS": "POINT OF SERVICE", "MVP": "MVP HEALTH CARE", "HAP": "HEALTH ALLIANCE PLAN",
    "PHP": "PHYSICIANS HEALTH PLAN", "QHP": "QUALIFIED HEALTH PLAN",
    "FEHB": "FEDERAL EMPLOYEES HEALTH BENEFITS", "CCHP": "CHINESE COMMUNITY HEALTH PLAN",
}

ADDR_STOPS = {'ST','STREET','AVE','AVENUE','BLVD','BOULEVARD','RD','ROAD',
              'DR','DRIVE','LN','LANE','CT','COURT','STE','SUITE','APT',
              'UNIT','FL','FLOOR','#'}

STREET_NORMS = {"STREET":"ST","AVENUE":"AVE","BOULEVARD":"BLVD","ROAD":"RD",
                "DRIVE":"DR","LANE":"LN","COURT":"CT","PLACE":"PL","CIRCLE":"CIR",
                "TERRACE":"TER","NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W",
                "NORTHEAST":"NE","NORTHWEST":"NW","SOUTHEAST":"SE","SOUTHWEST":"SW",
                "SUITE":"STE","APARTMENT":"APT"}

# ── Helpers ──────────────────────────────────────────────────────────────

def _up(s):
    return s.upper().strip() if s else None

def _digits(s):
    return re.sub(r'[^0-9]', '', s) if s else ''

def _ngrams(s, n=3):
    return [s[i:i+n] for i in range(len(s)-n+1)] if len(s) >= n else [s]

# ── Name strategies ──────────────────────────────────────────────────────

def _name_jw(a, b):
    return round(jellyfish.jaro_winkler_similarity(a, b), 4)

def _name_tsr(a, b):
    sa = ' '.join(sorted(a.split()))
    sb = ' '.join(sorted(b.split()))
    return round(SequenceMatcher(None, sa, sb).ratio(), 4)

def _name_pho(a, b):
    ma, mb = doublemetaphone(a), doublemetaphone(b)
    if (ma[0] and mb[0] and ma[0] == mb[0]) or \
       (ma[0] and mb[1] and ma[0] == mb[1]) or \
       (ma[1] and mb[0] and ma[1] == mb[0]):
        return 1.0
    return 0.0

def _name_abr(a, b):
    def expand(n):
        return ' '.join(ABBREVS.get(t, t) for t in n.split())
    return round(jellyfish.jaro_winkler_similarity(expand(a), expand(b)), 4)

def _name_ngr(a, b):
    ga, gb = Counter(_ngrams(a)), Counter(_ngrams(b))
    common = set(ga) & set(gb)
    dot = sum(ga[g]*gb[g] for g in common)
    ma = math.sqrt(sum(v*v for v in ga.values()))
    mb = math.sqrt(sum(v*v for v in gb.values()))
    return round(dot/(ma*mb), 4) if ma and mb else 0.0

# ── Address strategies ───────────────────────────────────────────────────

def _addr_jac(a, b):
    ta = set(t.rstrip('.,') for t in a.split()) - ADDR_STOPS
    tb = set(t.rstrip('.,') for t in b.split()) - ADDR_STOPS
    u = ta | tb
    return round(len(ta & tb)/len(u), 4) if u else 0.0

def _addr_cmp(a, b):
    def parse(addr):
        tok = addr.split()
        r = {}
        if tok and tok[0].replace('-','').isdigit():
            r['num'] = tok[0]; tok = tok[1:]
        r['name'] = ' '.join(t for t in tok[:3] if t not in ADDR_STOPS)
        return r
    ca, cb = parse(a), parse(b)
    tw, s = 0.0, 0.0
    for c, w in [('num',0.3),('name',0.3)]:
        va, vb = ca.get(c,''), cb.get(c,'')
        if va and vb:
            tw += w
            s += w * (1.0 if va == vb else jellyfish.jaro_winkler_similarity(va, vb))
    return round(s/tw, 4) if tw > 0 else 0.0

def _addr_nrm(a, b):
    def norm(addr):
        return ' '.join(STREET_NORMS.get(t, t) for t in addr.split())
    na, nb = norm(a), norm(b)
    return 1.0 if na == nb else round(jellyfish.jaro_winkler_similarity(na, nb), 4)

# ── Routing ──────────────────────────────────────────────────────────────

def _route_name(a, b):
    """Returns (score, strategy_name, all_strategies_tried)"""
    has_abbrev = any(t in ABBREVS for t in a.split()) or any(t in ABBREVS for t in b.split())
    len_ratio = min(len(a), len(b)) / max(len(a), len(b)) if max(len(a), len(b)) > 0 else 1.0

    results = {}

    if has_abbrev:
        results['NAME_ABR'] = _name_abr(a, b)
        results['NAME_JW'] = _name_jw(a, b)
        results['NAME_TSR'] = _name_tsr(a, b)
    elif len_ratio < 0.6:
        results['NAME_TSR'] = _name_tsr(a, b)
        results['NAME_ABR'] = _name_abr(a, b)
    elif max(len(a), len(b)) <= 20:
        results['NAME_JW'] = _name_jw(a, b)
        results['NAME_PHO'] = _name_pho(a, b)
        results['NAME_NGR'] = _name_ngr(a, b)
    else:
        results['NAME_JW'] = _name_jw(a, b)
        results['NAME_NGR'] = _name_ngr(a, b)

    winner = max(results, key=results.get)
    return results[winner], winner, list(results.keys())

def _route_addr(a, b):
    has_num = any(c.isdigit() for c in (a or '')[:10]) and any(c.isdigit() for c in (b or '')[:10])
    results = {}

    if has_num:
        results['ADDR_CMP'] = _addr_cmp(a, b)
        results['ADDR_NRM'] = _addr_nrm(a, b)
    else:
        results['ADDR_JAC'] = _addr_jac(a, b)

    winner = max(results, key=results.get)
    return results[winner], winner, list(results.keys())

# ── Main handler ─────────────────────────────────────────────────────────

def ensemble_match_score(name_a, name_b, tax_a, tax_b, addr_a, addr_b,
                         phone_a, phone_b, cms_a, cms_b):

    result = {
        'name_score': None, 'name_strategy': None,
        'addr_score': None, 'addr_strategy': None,
        'tax_score': None, 'phone_score': None, 'cms_score': None,
        'composite': None, 'strategies_used': [],
    }
    field_scores = {}
    strategies_used = []

    # ── Name ──
    if name_a and name_b:
        na, nb = _up(name_a), _up(name_b)
        score, strat, _ = _route_name(na, nb)
        result['name_score'] = score
        result['name_strategy'] = strat
        field_scores['name'] = score
        strategies_used.append(strat)

    # ── Tax ID ──
    if tax_a and tax_b:
        da, db = _digits(tax_a), _digits(tax_b)
        if da and db:
            exact = 1.0 if da == db else 0.0
            if exact == 1.0:
                result['tax_score'] = 1.0
                strategies_used.append('TAXID_EXACT')
            else:
                # Also try transpose detection
                trans = 0.0
                if len(da) == len(db):
                    diffs = sum(1 for x,y in zip(da,db) if x!=y)
                    trans = 0.9 if diffs <= 1 else (0.7 if diffs == 2 else 0.0)
                result['tax_score'] = max(exact, trans)
                strategies_used.append('TAXID_TRANSPOSE' if trans > exact else 'TAXID_EXACT')
            field_scores['tax_id'] = result['tax_score']  # runs on both paths

    # ── Address ──
    if addr_a and addr_b:
        aa, ab_ = _up(addr_a), _up(addr_b)
        score, strat, _ = _route_addr(aa, ab_)
        result['addr_score'] = score
        result['addr_strategy'] = strat
        field_scores['address'] = score
        strategies_used.append(strat)

    # ── Phone ──
    if phone_a and phone_b:
        pa, pb = _digits(phone_a), _digits(phone_b)
        if pa and pb:
            e164 = 1.0 if len(pa) >= 10 and len(pb) >= 10 and pa[-10:] == pb[-10:] else 0.0
            l7 = 1.0 if len(pa) >= 7 and len(pb) >= 7 and pa[-7:] == pb[-7:] else 0.0
            result['phone_score'] = max(e164, l7)
            strategies_used.append('PHONE_E164' if e164 >= l7 else 'PHONE_LAST7')
            field_scores['phone'] = result['phone_score']

    # ── CMS ──
    if cms_a and cms_b:
        ca, cb = _up(cms_a), _up(cms_b)
        exact = 1.0 if ca == cb else 0.0
        pfx = 0.85 if len(ca)>=5 and len(cb)>=5 and ca[:5]==cb[:5] else 0.0
        result['cms_score'] = max(exact, pfx)
        strategies_used.append('CMS_EXACT' if exact >= pfx else 'CMS_PREFIX')
        field_scores['cms'] = result['cms_score']

    # ── Composite (weighted) ──
    WEIGHTS = {'name': 0.35, 'tax_id': 0.25, 'address': 0.20, 'phone': 0.10, 'cms': 0.10}
    total_w = sum(WEIGHTS[f] for f in field_scores)
    if total_w > 0:
        result['composite'] = round(sum(field_scores[f]*WEIGHTS[f] for f in field_scores) / total_w, 4)

    result['strategies_used'] = strategies_used
    return result
$$;
