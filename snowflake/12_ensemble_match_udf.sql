-- ===========================================================================
-- Ensemble Match Score UDF — MDM.MATCH.ENSEMBLE_MATCH_SCORE
-- ---------------------------------------------------------------------------
-- Data-characteristic router: inspects each field pair, selects 2-3
-- appropriate strategies, runs them, returns rich result with per-field
-- scores, winning strategy, routing reason, and weighted composite.
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

PAYOR_ABBREVIATIONS = {
    "BCBS": "BLUE CROSS BLUE SHIELD", "BC": "BLUE CROSS", "BS": "BLUE SHIELD",
    "UHC": "UNITEDHEALTHCARE", "UHG": "UNITEDHEALTH GROUP",
    "HCSC": "HEALTH CARE SERVICE CORPORATION", "KP": "KAISER PERMANENTE",
    "KFHP": "KAISER FOUNDATION HEALTH PLAN", "HMO": "HEALTH MAINTENANCE ORGANIZATION",
    "PPO": "PREFERRED PROVIDER ORGANIZATION", "EPO": "EXCLUSIVE PROVIDER ORGANIZATION",
    "POS": "POINT OF SERVICE", "MVP": "MVP HEALTH CARE", "HAP": "HEALTH ALLIANCE PLAN",
    "PHP": "PHYSICIANS HEALTH PLAN", "QHP": "QUALIFIED HEALTH PLAN",
    "FEHB": "FEDERAL EMPLOYEES HEALTH BENEFITS", "CCHP": "CHINESE COMMUNITY HEALTH PLAN",
}

ADDR_STOPWORDS = {'ST','STREET','AVE','AVENUE','BLVD','BOULEVARD','RD','ROAD',
                  'DR','DRIVE','LN','LANE','CT','COURT','STE','SUITE','APT',
                  'UNIT','FL','FLOOR','#'}

STREET_NORMS = {"STREET":"ST","AVENUE":"AVE","BOULEVARD":"BLVD","ROAD":"RD",
                "DRIVE":"DR","LANE":"LN","COURT":"CT","PLACE":"PL","CIRCLE":"CIR",
                "TERRACE":"TER","NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W",
                "NORTHEAST":"NE","NORTHWEST":"NW","SOUTHEAST":"SE","SOUTHWEST":"SW",
                "SUITE":"STE","APARTMENT":"APT"}

# ── Helpers ──────────────────────────────────────────────────────────────

def _clean(s):
    return s.upper().strip() if s else None

def _digits(s):
    return re.sub(r'[^0-9]', '', s) if s else None

def _ngrams(s, n=3):
    s = s.upper()
    return [s[i:i+n] for i in range(len(s)-n+1)] if len(s) >= n else [s]

# ── Name strategies ──────────────────────────────────────────────────────

def _name_jw(a, b):
    return jellyfish.jaro_winkler_similarity(a, b)

def _name_tsr(a, b):
    sa = ' '.join(sorted(a.split()))
    sb = ' '.join(sorted(b.split()))
    return SequenceMatcher(None, sa, sb).ratio()

def _name_pho(a, b):
    ma = doublemetaphone(a)
    mb = doublemetaphone(b)
    if (ma[0] and mb[0] and ma[0] == mb[0]) or \
       (ma[0] and mb[1] and ma[0] == mb[1]) or \
       (ma[1] and mb[0] and ma[1] == mb[0]):
        return 1.0
    return 0.0

def _expand_abbr(name):
    return ' '.join(PAYOR_ABBREVIATIONS.get(t, t) for t in name.split())

def _name_abr(a, b):
    return jellyfish.jaro_winkler_similarity(_expand_abbr(a), _expand_abbr(b))

def _name_ngr(a, b):
    ga = Counter(_ngrams(a))
    gb = Counter(_ngrams(b))
    common = set(ga) & set(gb)
    dot = sum(ga[g] * gb[g] for g in common)
    ma = math.sqrt(sum(v*v for v in ga.values()))
    mb = math.sqrt(sum(v*v for v in gb.values()))
    return dot / (ma * mb) if ma and mb else 0.0

NAME_FNS = {
    'NAME_JW': _name_jw, 'NAME_TSR': _name_tsr, 'NAME_PHO': _name_pho,
    'NAME_ABR': _name_abr, 'NAME_NGR': _name_ngr,
}

# ── Address strategies ───────────────────────────────────────────────────

def _addr_jac(a, b):
    ta = set(t.rstrip('.,') for t in a.split()) - ADDR_STOPWORDS
    tb = set(t.rstrip('.,') for t in b.split()) - ADDR_STOPWORDS
    union = ta | tb
    return len(ta & tb) / len(union) if union else 0.0

def _parse_addr(addr):
    tokens = addr.split()
    r = {}
    if tokens and tokens[0].replace('-','').isdigit():
        r['num'] = tokens[0]
        tokens = tokens[1:]
    r['name'] = ' '.join(t for t in tokens[:3] if t not in ADDR_STOPWORDS)
    return r

def _addr_cmp(a, b):
    ca, cb = _parse_addr(a), _parse_addr(b)
    tw, s = 0.0, 0.0
    for comp, w in [('num', 0.30), ('name', 0.30)]:
        va, vb = ca.get(comp, ''), cb.get(comp, '')
        if va and vb:
            tw += w
            s += w * (1.0 if va == vb else jellyfish.jaro_winkler_similarity(va, vb))
    return s / tw if tw > 0 else 0.0

def _addr_zip(a, b, za, zb):
    if not za or not zb:
        return 0.0
    z = 1.0 if za == zb else (0.5 if za[:3] == zb[:3] else 0.0)
    j = _addr_jac(a, b) if a and b else 0.0
    return z * 0.5 + j * 0.5

def _addr_nrm(a, b):
    def norm(addr):
        return ' '.join(STREET_NORMS.get(t, t) for t in addr.split())
    na, nb = norm(a), norm(b)
    return 1.0 if na == nb else jellyfish.jaro_winkler_similarity(na, nb)

# ── Routing logic ────────────────────────────────────────────────────────

def _route_name(a, b):
    strats = ['NAME_JW']
    reason = 'default'

    has_abbrev = any(t in PAYOR_ABBREVIATIONS for t in a.split()) or \
                 any(t in PAYOR_ABBREVIATIONS for t in b.split())
    short = min(len(a), len(b))
    len_diff = abs(len(a) - len(b))

    if has_abbrev or (len_diff > short * 0.5 and short <= 10):
        strats.append('NAME_ABR')
        reason = 'abbreviation_detected'

    ta = set(a.split())
    tb = set(b.split())
    if len(ta) >= 2 and len(tb) >= 2:
        overlap = len(ta & tb) / max(len(ta | tb), 1)
        if overlap > 0.3:
            strats.append('NAME_TSR')
            if reason == 'default':
                reason = 'token_overlap'

    if max(len(a), len(b)) <= 20:
        strats.append('NAME_PHO')
        if reason == 'default':
            reason = 'short_names_phonetic'

    if min(len(a), len(b)) > 25:
        strats.append('NAME_NGR')
        if reason == 'default':
            reason = 'long_names_ngram'

    return strats[:3], reason

def _route_addr(a, b, za, zb):
    strats = ['ADDR_JAC']
    reason = 'default'

    if any(c.isdigit() for c in (a or '')[:10]) and any(c.isdigit() for c in (b or '')[:10]):
        strats.append('ADDR_CMP')
        reason = 'structured_addresses'

    if za and zb:
        strats.append('ADDR_ZIP')
        if reason == 'default':
            reason = 'zips_available'

    strats.append('ADDR_NRM')
    return strats[:3], reason

# ── Main ─────────────────────────────────────────────────────────────────

def ensemble_match_score(name_a, name_b, tax_a, tax_b, addr_a, addr_b,
                         phone_a, phone_b, cms_a, cms_b):
    result = {}
    weights = {}

    # ── Name ──
    if name_a and name_b:
        na, nb = _clean(name_a), _clean(name_b)
        strats, reason = _route_name(na, nb)
        scores = {s: round(NAME_FNS[s](na, nb), 4) for s in strats}
        winner = max(scores, key=scores.get)
        result['name'] = {
            'selected_strategies': strats,
            'strategy_scores': scores,
            'winning_strategy': winner,
            'score': scores[winner],
            'routing_reason': reason,
        }
        weights['name'] = 0.35

    # ── Tax ID ──
    if tax_a and tax_b:
        da, db = _digits(tax_a), _digits(tax_b)
        if da and db:
            scores = {'TIN_EXACT': 1.0 if da == db else 0.0}
            reason = 'exact_match'
            if da != db:
                reason = 'exact_failed_trying_fuzzy'
                if len(da) == len(db):
                    diffs = sum(1 for x, y in zip(da, db) if x != y)
                    scores['TIN_TRANS'] = 0.9 if diffs <= 1 else (0.7 if diffs == 2 else 0.0)
                if len(da) >= 5 and len(db) >= 5 and da[:2] == db[:2]:
                    matching = sum(1 for x, y in zip(da[2:], db[2:]) if x == y)
                    scores['TIN_PFX'] = 0.6 if matching >= 5 else 0.0
            winner = max(scores, key=scores.get)
            result['tax_id'] = {
                'selected_strategies': list(scores.keys()),
                'strategy_scores': scores,
                'winning_strategy': winner,
                'score': scores[winner],
                'routing_reason': reason,
            }
            weights['tax_id'] = 0.25

    # ── Address ──
    if addr_a and addr_b:
        aa, ab_ = _clean(addr_a), _clean(addr_b)
        za = _digits(addr_a.split()[-1]) if addr_a else None  # crude zip extraction
        zb = _digits(addr_b.split()[-1]) if addr_b else None
        # Use phone params as zip if they look like zips — but spec says addr includes zip in concat
        strats, reason = _route_addr(aa, ab_, za, zb)
        scores = {}
        for s in strats:
            if s == 'ADDR_JAC':
                scores[s] = round(_addr_jac(aa, ab_), 4)
            elif s == 'ADDR_CMP':
                scores[s] = round(_addr_cmp(aa, ab_), 4)
            elif s == 'ADDR_ZIP':
                scores[s] = round(_addr_zip(aa, ab_, za, zb), 4)
            elif s == 'ADDR_NRM':
                scores[s] = round(_addr_nrm(aa, ab_), 4)
        winner = max(scores, key=scores.get)
        result['address'] = {
            'selected_strategies': strats,
            'strategy_scores': scores,
            'winning_strategy': winner,
            'score': scores[winner],
            'routing_reason': reason,
        }
        weights['address'] = 0.20

    # ── Phone ──
    if phone_a and phone_b:
        pa, pb = _digits(phone_a), _digits(phone_b)
        if pa and pb:
            l7 = 1.0 if len(pa) >= 7 and len(pb) >= 7 and pa[-7:] == pb[-7:] else 0.0
            e164 = 1.0 if len(pa) >= 10 and len(pb) >= 10 and pa[-10:] == pb[-10:] else 0.0
            ac = l7 if l7 == 1.0 else (0.5 if len(pa) >= 10 and len(pb) >= 10 and pa[-4:] == pb[-4:] else 0.0)
            scores = {'PHONE_L7': l7, 'PHONE_E164': e164, 'PHONE_AC': round(ac, 4)}
            winner = max(scores, key=scores.get)
            result['phone'] = {
                'selected_strategies': list(scores.keys()),
                'strategy_scores': scores,
                'winning_strategy': winner,
                'score': scores[winner],
                'routing_reason': 'default',
            }
            weights['phone'] = 0.10

    # ── CMS Plan ID ──
    if cms_a and cms_b:
        ca, cb = _clean(cms_a), _clean(cms_b)
        scores = {'CMS_EXACT': 1.0 if ca == cb else 0.0}
        if ca != cb and len(ca) >= 5 and len(cb) >= 5:
            scores['CMS_PFX'] = 0.85 if ca[:5] == cb[:5] else 0.0
        winner = max(scores, key=scores.get)
        result['cms_plan_id'] = {
            'selected_strategies': list(scores.keys()),
            'strategy_scores': scores,
            'winning_strategy': winner,
            'score': scores[winner],
            'routing_reason': 'default',
        }
        weights['cms_plan_id'] = 0.10

    # ── Composite ──
    total_weight = sum(weights.values())
    composite = sum(result[f]['score'] * weights[f] for f in weights) / total_weight if total_weight > 0 else 0.0

    return {
        **result,
        'composite': round(composite, 4),
        'field_weights': {k: round(v, 2) for k, v in weights.items()},
    }
$$;
