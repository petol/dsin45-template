{{ config(materialized='table') }}

with deals as (
    select * from {{ ref('stg_deals') }}
),

companies as (
    select * from {{ ref('stg_companies') }}
),

-- Parse companies array from deals to get company associations
deal_companies as (
    select 
        deal_id,
        case 
            when companies is not null and companies != 'NULL' and companies != '' 
            then replace(replace(replace(companies, '[', ''), ']', ''), '"', '')
            else null 
        end as company_id_str
    from deals
),

-- Convert company_id string to actual company_id (assuming single company per deal for now)
deal_company_parsed as (
    select 
        deal_id,
        case 
            when company_id_str is not null and company_id_str ~ '^[0-9]+$'
            then company_id_str::bigint
            else null
        end as company_id
    from deal_companies
),

fact_deals as (
    select
        -- Deal identifiers
        d.deal_id,
        d.hubspot_object_id,
        
        -- Company dimensions
        coalesce(dcp.company_id, c.company_id) as company_id,
        c.company_name,
        c.industry,
        c.domain,
        c.city as company_city,
        c.state as company_state,
        c.country as company_country,
        
        -- Deal dimensions
        d.deal_name,
        d.deal_stage,
        d.pipeline,
        d.deal_type,
        d.is_closed,
        d.is_closed_won,
        d.is_closed_lost,
        
        -- Date dimensions
        d.created_at::date as deal_created_date,
        d.close_date::date as deal_close_date,
        d.closed_won_date::date as deal_closed_won_date,
        extract(year from d.created_at) as deal_created_year,
        extract(quarter from d.created_at) as deal_created_quarter,
        extract(month from d.created_at) as deal_created_month,
        extract(year from d.close_date) as deal_close_year,
        extract(quarter from d.close_date) as deal_close_quarter,
        extract(month from d.close_date) as deal_close_month,
        
        -- Financial metrics
        d.amount,
        d.closed_amount,
        d.forecast_amount,
        d.projected_amount,
        d.annual_contract_value,
        d.monthly_recurring_revenue,
        d.total_contract_value,
        
        -- Additional metrics
        d.days_to_close,
        d.deal_stage_probability,
        
        -- Owner information
        d.hubspot_owner_id as deal_owner_id,
        c.hubspot_owner_id as company_owner_id,
        
        -- Metadata
        d.was_imported,
        d.object_source,
        
        -- Metrics (always 1 for each deal record to enable counting)
        1 as number_of_deals

    from deals d
    left join deal_company_parsed dcp on d.deal_id = dcp.deal_id
    left join companies c on dcp.company_id = c.company_id
    
    -- Include deals even without company association
    where not coalesce(d.archived, false)
)

select * from fact_deals