INSERT INTO registry_sources (
    source_id,
    source_name,
    entity_type,
    county,
    source_url,
    priority_tier,
    website_ready
)
VALUES
(
    'state-njdot-construction',
    'NJDOT Construction Services',
    'State Agency',
    'Statewide',
    'https://www.nj.gov/transportation/business/procurement/ConstrServ/curradvproj.shtm',
    'Tier 1',
    'Yes'
),
(
    'state-njdot-profserv',
    'NJDOT Professional Services',
    'State Agency',
    'Statewide',
    'https://www.nj.gov/transportation/business/procurement/ProfServ/CurrentSolic.shtm',
    'Tier 1',
    'Yes'
),
(
    'state-njta',
    'NJ Turnpike Authority Current Solicitations',
    'Transportation Authority',
    'Statewide',
    'https://www.njta.gov/business-hub/current-solicitations/',
    'Tier 1',
    'Yes'
)
ON CONFLICT (source_id) DO NOTHING;
