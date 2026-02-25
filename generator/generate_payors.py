#!/opt/homebrew/bin/python3.11
"""
Payor MDM Data Generator
-------------------------
Generates ~150-200 source records across 4 CSVs simulating messy multi-source
payor data for the MDM matching engine.

Based on ~65 real payor entities with intentional duplicates, name variants,
address inconsistencies, missing fields, conflicting tax IDs, stale status,
and parent/child confusion.

Usage:
  python3.11 generate_payors.py
"""

import csv
import json
import os
import random
import sys
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

from mess_patterns import (
    name_variant, address_variant, single_line_address, state_variant,
    zip_variant, phone_variant, maybe_null, tax_id_variant,
    maybe_stale_status, BCBS_VARIANTS,
)

# Reproducibility
SEED = 42
random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

OUTPUT_DIR = Path(__file__).parent / "output"
FAMILIES_FILE = Path(__file__).parent / "payor_families.json"

# Source appearance probabilities
SOURCE_PROBS = {
    "crm": 0.90,
    "claims": 0.80,
    "credentialing": 0.60,
    "cms_reference": 0.40,
}


# ---------------------------------------------------------------------------
# Canonical entity builder
# ---------------------------------------------------------------------------

def build_canonical_entities(families: list[dict]) -> list[dict]:
    """Build ~65 canonical payor entities from family definitions."""
    entities = []
    eid = 0

    for fam in families:
        parent_name = fam.get("parent")
        parent_tax = fam.get("tax_id")
        parent_state = fam.get("state", "")
        parent_city = fam.get("city", "")

        # Add parent as an entity if it exists
        if parent_name:
            eid += 1
            entities.append({
                "entity_id": eid,
                "family_id": fam["family_id"],
                "name": parent_name,
                "tax_id": parent_tax,
                "state": parent_state,
                "city": parent_city,
                "address": fake.street_address(),
                "zip": fake.zipcode_in_state(parent_state) if parent_state else fake.zipcode(),
                "phone": fake.phone_number(),
                "website": f"https://www.{parent_name.lower().replace(' ', '').replace(',', '')[:20]}.com",
                "type": "commercial",
                "lob": "PPO",
                "is_parent": True,
                "parent_entity_id": None,
                "parent_name": None,
                "is_active": True,
                "effective_date": date(2000 + random.randint(0, 15), 1, 1),
                "dba": None,
                "cms_plan_id": None,
                "npi": f"1{random.randint(100000000, 999999999)}" if random.random() < 0.3 else None,
            })
            parent_eid = eid

        for ent in fam["entities"]:
            eid += 1
            st = ent.get("state", parent_state)
            ct = ent.get("city", parent_city)
            entities.append({
                "entity_id": eid,
                "family_id": fam["family_id"],
                "name": ent["name"],
                "tax_id": ent.get("tax_id", parent_tax),
                "state": st,
                "city": ct or fake.city(),
                "address": fake.street_address(),
                "zip": fake.zipcode_in_state(st) if st else fake.zipcode(),
                "phone": fake.phone_number(),
                "website": f"https://www.{ent['name'].lower().replace(' ', '')[:20]}.com",
                "type": ent.get("type", "commercial"),
                "lob": ent.get("lob", "PPO"),
                "is_parent": False,
                "parent_entity_id": parent_eid if parent_name else None,
                "parent_name": parent_name,
                "is_active": random.random() > 0.08,
                "effective_date": date(2000 + random.randint(0, 20), random.randint(1, 12), 1),
                "dba": ent.get("dba"),
                "cms_plan_id": ent.get("cms_plan_id"),
                "npi": f"1{random.randint(100000000, 999999999)}" if random.random() < 0.2 else None,
            })

    return entities


# ---------------------------------------------------------------------------
# Source record generators
# ---------------------------------------------------------------------------

def gen_crm_record(entity: dict, idx: int) -> dict:
    """Generate a CRM source record."""
    src = "crm"
    return {
        "src_payor_id": f"CRM-{idx:04d}",
        "payor_name": name_variant(entity["name"], src),
        "payor_name_2": entity.get("dba") or (entity["parent_name"] if random.random() < 0.2 else None),
        "tax_id": tax_id_variant(entity["tax_id"], src),
        "npi": entity.get("npi"),
        "cms_plan_id": entity.get("cms_plan_id"),
        "address_line_1": address_variant(entity["address"]),
        "address_line_2": f"Suite {random.randint(100, 999)}" if random.random() < 0.3 else None,
        "city": entity["city"],
        "state": entity["state"],
        "zip": entity["zip"],
        "phone": phone_variant(entity["phone"]),
        "website": maybe_null(entity["website"], 0.1),
        "payor_type": entity["type"],
        "parent_payor_id": f"CRM-{entity['parent_entity_id']:04d}" if entity.get("parent_entity_id") and random.random() < 0.6 else None,
        "status": "active" if entity["is_active"] else random.choice(["inactive", "terminated"]),
        "effective_date": entity["effective_date"].isoformat(),
        "termination_date": (entity["effective_date"] + timedelta(days=random.randint(365, 3650))).isoformat() if not entity["is_active"] else None,
    }


def gen_claims_record(entity: dict, idx: int) -> dict:
    """Generate a claims source record — messiest."""
    src = "claims"
    addr = single_line_address(
        address_variant(entity["address"]),
        f"Ste {random.randint(100, 999)}" if random.random() < 0.2 else None,
        entity["city"],
        state_variant(entity["state"], src),
        zip_variant(entity["zip"], src),
    )
    return {
        "claims_payor_code": f"CLM-{idx:04d}",
        "payor_name": name_variant(entity["name"], src),
        "tax_id": tax_id_variant(maybe_null(entity["tax_id"], 0.15), src),
        "address": addr,
        "city": maybe_null(entity["city"], 0.1),
        "state": state_variant(entity["state"], src),
        "zip": zip_variant(entity["zip"], src),
        "payor_type": maybe_null(entity["type"], 0.1),
        "line_of_business": entity["lob"],
        "is_active": maybe_stale_status(entity["is_active"], src),
    }


def gen_credentialing_record(entity: dict, idx: int) -> dict:
    """Generate a credentialing source record."""
    src = "credentialing"
    return {
        "cred_payor_id": f"CRED-{idx:04d}",
        "organization_name": name_variant(entity["name"], src),
        "doing_business_as": entity.get("dba") or (name_variant(entity["name"], "claims") if random.random() < 0.15 else None),
        "ein": tax_id_variant(maybe_null(entity["tax_id"], 0.2), src),
        "street_address": address_variant(entity["address"]),
        "suite": f"Suite {random.randint(100, 999)}" if random.random() < 0.25 else None,
        "city": entity["city"],
        "state_code": entity["state"],
        "postal_code": zip_variant(entity["zip"], src),
        "contact_phone": phone_variant(maybe_null(entity["phone"], 0.2)),
        "contact_email": maybe_null(f"contracts@{entity['name'].lower().replace(' ', '')[:15]}.com", 0.3),
        "plan_type": entity["lob"],
        "network_status": random.choice(["in_network", "in_network", "in_network", "out_of_network", "pending"]),
    }


def gen_reference_record(entity: dict, idx: int) -> dict:
    """Generate a CMS reference data record."""
    src = "cms_reference"
    return {
        "ref_id": f"REF-{idx:04d}",
        "official_name": name_variant(entity["name"], src),
        "parent_org_name": entity.get("parent_name") if random.random() < 0.7 else None,
        "tax_id": entity["tax_id"],  # reference data usually clean
        "cms_contract_id": entity.get("cms_plan_id"),
        "plan_type": entity["type"],
        "state": entity["state"],
        "enrollment_count": random.randint(1000, 5000000) if random.random() < 0.8 else None,
        "star_rating": round(random.uniform(2.0, 5.0), 1) if entity.get("cms_plan_id") else None,
        "source_url": f"https://data.cms.gov/plans/{entity.get('cms_plan_id', 'NA')}",
    }


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate():
    with open(FAMILIES_FILE) as f:
        families = json.load(f)

    entities = build_canonical_entities(families)
    print(f"Built {len(entities)} canonical entities from {len(families)} families")

    crm_records = []
    claims_records = []
    cred_records = []
    ref_records = []

    crm_idx = claims_idx = cred_idx = ref_idx = 0

    for entity in entities:
        # Determine which sources have this entity
        if random.random() < SOURCE_PROBS["crm"]:
            crm_idx += 1
            crm_records.append(gen_crm_record(entity, crm_idx))

            # 5% chance of within-source duplicate
            if random.random() < 0.05:
                crm_idx += 1
                dup = gen_crm_record(entity, crm_idx)
                dup["payor_name"] = name_variant(entity["name"], "claims")  # slightly different name
                crm_records.append(dup)

        if random.random() < SOURCE_PROBS["claims"]:
            claims_idx += 1
            claims_records.append(gen_claims_record(entity, claims_idx))

        if random.random() < SOURCE_PROBS["credentialing"]:
            cred_idx += 1
            cred_records.append(gen_credentialing_record(entity, cred_idx))

        if random.random() < SOURCE_PROBS["cms_reference"]:
            ref_idx += 1
            ref_records.append(gen_reference_record(entity, ref_idx))

    # Add 5-8 orphan records (no match possible — only in one source)
    for i in range(random.randint(5, 8)):
        crm_idx += 1
        orphan_name = f"{fake.company()} Health Plan"
        orphan_state = fake.state_abbr()
        crm_records.append({
            "src_payor_id": f"CRM-{crm_idx:04d}",
            "payor_name": orphan_name,
            "payor_name_2": None,
            "tax_id": f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}",
            "npi": None,
            "cms_plan_id": None,
            "address_line_1": fake.street_address(),
            "address_line_2": None,
            "city": fake.city(),
            "state": orphan_state,
            "zip": fake.zipcode(),
            "phone": fake.phone_number(),
            "website": None,
            "payor_type": random.choice(["commercial", "exchange"]),
            "parent_payor_id": None,
            "status": "active",
            "effective_date": date(2022, 1, 1).isoformat(),
            "termination_date": None,
        })

    # Add some parent/child confusion: child listed as standalone in claims
    confused_count = 0
    for entity in entities:
        if entity.get("parent_name") and random.random() < 0.25 and confused_count < 8:
            claims_idx += 1
            rec = gen_claims_record(entity, claims_idx)
            # Use parent's name instead of child's — parent/child confusion
            rec["payor_name"] = name_variant(entity["parent_name"], "claims")
            rec["tax_id"] = tax_id_variant(entity["tax_id"], "claims")
            claims_records.append(rec)
            confused_count += 1

    total = len(crm_records) + len(claims_records) + len(cred_records) + len(ref_records)
    print(f"\nGenerated {total} source records:")
    print(f"  CRM:            {len(crm_records)}")
    print(f"  Claims:         {len(claims_records)}")
    print(f"  Credentialing:  {len(cred_records)}")
    print(f"  Reference:      {len(ref_records)}")

    # Write CSVs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def write_csv(filename: str, records: list[dict]):
        if not records:
            return
        path = OUTPUT_DIR / filename
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
        print(f"  → {path} ({len(records)} rows)")

    print(f"\nWriting CSVs to {OUTPUT_DIR}/")
    write_csv("src_crm_payors.csv", crm_records)
    write_csv("src_claims_payors.csv", claims_records)
    write_csv("src_credentialing_payors.csv", cred_records)
    write_csv("src_reference_payors.csv", ref_records)

    print(f"\n✅ Done. {total} records across 4 CSVs representing ~{len(entities)} entities.")


if __name__ == "__main__":
    generate()
