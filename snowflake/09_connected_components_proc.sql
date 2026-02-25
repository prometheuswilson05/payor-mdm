-- ===========================================================================
-- Snowpark Python Stored Procedure: MDM.MATCH.BUILD_MATCH_GROUPS
-- ---------------------------------------------------------------------------
-- Reads confirmed match pairs from MATCH_CANDIDATES, builds connected
-- components using networkx, and writes group assignments to MATCH_GROUPS.
--
-- For each group:
--   - match_group_id = deterministic UUID5 from sorted source_record_ids
--   - is_survivor = TRUE for the record with lowest source_trust_rank
--   - group_confidence = average match score within the group
-- ===========================================================================

USE DATABASE MDM;
USE SCHEMA MATCH;

CREATE OR REPLACE PROCEDURE MDM.MATCH.BUILD_MATCH_GROUPS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'networkx')
HANDLER = 'build_match_groups'
AS
$$
import uuid
import networkx as nx

def build_match_groups(session):
    """Build connected components from confirmed match pairs."""

    # 1. Read confirmed match edges
    edges_df = session.sql("""
        SELECT source_record_id_a, source_record_id_b, overall_score
        FROM MDM.MATCH.MATCH_CANDIDATES
        WHERE final_decision = 'match'
    """).collect()

    # 2. Read all source records with trust rank (for survivor selection + singletons)
    all_records_df = session.sql("""
        SELECT source_record_id, source_trust_rank
        FROM MDM.STAGING.STG_PAYORS_UNIONED
    """).collect()

    trust_map = {r['SOURCE_RECORD_ID']: r['SOURCE_TRUST_RANK'] for r in all_records_df}
    all_record_ids = set(trust_map.keys())

    # 3. Build graph and find connected components
    G = nx.Graph()
    G.add_nodes_from(all_record_ids)

    edge_scores = {}
    for row in edges_df:
        a, b, score = row['SOURCE_RECORD_ID_A'], row['SOURCE_RECORD_ID_B'], row['OVERALL_SCORE']
        G.add_edge(a, b)
        edge_scores[(a, b)] = score
        edge_scores[(b, a)] = score

    components = list(nx.connected_components(G))

    # 4. Build group rows
    rows = []
    for comp in components:
        members = sorted(comp)

        # Deterministic UUID5 from sorted member IDs
        seed_str = '|'.join(members)
        group_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, seed_str))

        # Average match score within this group
        group_scores = []
        for a in members:
            for b in members:
                if (a, b) in edge_scores:
                    group_scores.append(edge_scores[(a, b)])
        avg_confidence = sum(group_scores) / len(group_scores) if group_scores else None

        # Survivor = lowest trust rank (1=most trusted)
        survivor_id = min(members, key=lambda m: trust_map.get(m, 999))

        for member in members:
            rows.append({
                'match_group_id': group_id,
                'source_record_id': member,
                'is_survivor': member == survivor_id,
                'group_confidence': round(avg_confidence, 4) if avg_confidence else None,
            })

    # 5. Truncate and insert
    session.sql("DELETE FROM MDM.MATCH.MATCH_GROUPS").collect()

    if rows:
        # Insert in batches via SQL VALUES
        batch_size = 100
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            values_clauses = []
            for r in batch:
                conf = str(r['group_confidence']) if r['group_confidence'] is not None else 'NULL'
                values_clauses.append(
                    f"('{r['match_group_id']}', '{r['source_record_id']}', "
                    f"{r['is_survivor']}, {conf})"
                )
            values_str = ',\n'.join(values_clauses)
            session.sql(f"""
                INSERT INTO MDM.MATCH.MATCH_GROUPS
                    (match_group_id, source_record_id, is_survivor, group_confidence)
                VALUES {values_str}
            """).collect()

    group_count = len([c for c in components if len(c) > 1])
    singleton_count = len([c for c in components if len(c) == 1])
    total_rows = len(rows)

    return f"Done: {total_rows} rows, {group_count} multi-member groups, {singleton_count} singletons"
$$;
