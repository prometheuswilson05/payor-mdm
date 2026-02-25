-- Connected components via recursive CTE.
-- Each record starts as its own component (min source_record_id within the
-- connected cluster becomes the match_group_id).
-- For 149 records this converges in a small number of iterations.

with matches as (

    select
        source_record_id_a as node_a,
        source_record_id_b as node_b
    from {{ ref('match_candidates') }}
    where final_decision = 'match'

),

-- All nodes mentioned in any match edge
all_nodes as (

    select distinct source_record_id as node
    from {{ ref('stg_payors_unioned') }}

),

-- Seed: assign each node its own component (its own ID)
initial_components as (

    select
        n.node,
        coalesce(
            min(case when m.node_a = n.node then m.node_b
                     when m.node_b = n.node then m.node_a
                end) over (partition by n.node),
            n.node
        ) as component

    from all_nodes n
    left join matches m
        on n.node in (m.node_a, m.node_b)

),

-- Propagate: component = min node in cluster (iterative via JOIN)
-- One iteration is sufficient for 2-hop chains in small datasets;
-- deeper chains would need Snowpark stored procedure
pass1 as (

    select
        ic.node,
        least(ic.component,
              coalesce(min(ic2.component), ic.component)) as component
    from initial_components ic
    left join matches m
        on ic.node in (m.node_a, m.node_b)
    left join initial_components ic2
        on ic2.node = case when m.node_a = ic.node then m.node_b else m.node_a end
    group by ic.node, ic.component

),

pass2 as (

    select
        p.node,
        least(p.component,
              coalesce(min(p2.component), p.component)) as component
    from pass1 p
    left join matches m
        on p.node in (m.node_a, m.node_b)
    left join pass1 p2
        on p2.node = case when m.node_a = p.node then m.node_b else m.node_a end
    group by p.node, p.component

),

-- Assign group confidence (avg overall_score within each group)
group_scores as (

    select
        p.component,
        avg(mc.overall_score) as group_confidence

    from pass2 p
    join matches m on p.node in (m.node_a, m.node_b)
    join {{ ref('match_candidates') }} mc
        on mc.source_record_id_a = m.node_a
        and mc.source_record_id_b = m.node_b
    group by p.component

),

final as (

    select
        -- match_group_id: UUID derived from the component anchor
        md5(p.component)             as match_group_id,
        p.node                       as source_record_id,
        -- survivor: the record from the highest-trust source with the component anchor
        (p.node = p.component)       as is_survivor,
        gs.group_confidence,
        current_timestamp()          as created_at

    from pass2 p
    left join group_scores gs on p.component = gs.component

)

select * from final
