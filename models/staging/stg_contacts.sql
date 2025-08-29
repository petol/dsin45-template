{{ config(materialized='view') }}

with source as (
    select * from {{ source('lake', 'contacts') }}
),

renamed as (
    select
        id as contact_id,
        archived,
        companies,
        "createdAt" as created_at,
        "updatedAt" as updated_at,
        
        -- Core contact properties
        properties_firstname as first_name,
        properties_lastname as last_name,
        properties_email as email,
        properties_phone as phone,
        properties_mobilephone as mobile_phone,
        properties_work_email as work_email,
        
        -- Address information
        properties_address as address,
        properties_city as city,
        properties_state as state,
        properties_zip as zip_code,
        properties_country as country,
        
        -- Company information
        properties_company as company,
        properties_industry as industry,
        properties_jobtitle as job_title,
        properties_numemployees as number_of_employees,
        properties_annualrevenue as annual_revenue,
        properties_company_size as company_size,
        
        -- HubSpot specific fields
        properties_hs_object_id as hubspot_object_id,
        properties_hubspot_owner_id as hubspot_owner_id,
        properties_lifecyclestage as lifecycle_stage,
        properties_hs_lead_status as lead_status,
        properties_hs_analytics_source as analytics_source,
        properties_hs_latest_source as latest_source,
        properties_createdate as hubspot_create_date,
        properties_lastmodifieddate as last_modified_date,
        properties_closedate as close_date,
        
        -- Engagement metrics
        properties_hubspotscore as hubspot_score,
        properties_hs_analytics_num_visits as analytics_visits,
        properties_hs_analytics_num_page_views as analytics_page_views,
        properties_hs_email_delivered as email_delivered,
        properties_hs_email_open as email_opened,
        properties_hs_email_click as email_clicked,
        properties_hs_email_replied as email_replied,
        properties_num_notes as number_of_notes,
        properties_num_associated_deals as number_of_associated_deals,
        
        -- Additional properties
        properties_degree as degree,
        properties_school as school,
        properties_field_of_study as field_of_study,
        properties_graduation_date as graduation_date,
        properties_job_function as job_function,
        properties_seniority as seniority,
        properties_hs_persona as persona,
        properties_marital_status as marital_status,
        properties_date_of_birth as date_of_birth,
        
        -- Metadata columns
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id,
        _airbyte_meta

    from source
)

select * from renamed