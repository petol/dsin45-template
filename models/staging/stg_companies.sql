{{ config(materialized='view') }}

with source as (
    select * from {{ source('lake', 'companies') }}
),

renamed as (
    select
        id as company_id,
        archived,
        contacts,
        "createdAt" as created_at,
        "updatedAt" as updated_at,
        
        -- Core company information
        properties_name as company_name,
        properties_domain as domain,
        properties_website as website,
        properties_description as description,
        properties_about_us as about_us,
        properties_industry as industry,
        properties_type as company_type,
        
        -- Contact and address information
        properties_phone as phone,
        properties_address as address,
        properties_address2 as address_2,
        properties_city as city,
        properties_state as state,
        properties_zip as zip_code,
        properties_country as country,
        properties_timezone as timezone,
        properties_hs_state_code as state_code,
        properties_hs_country_code as country_code,
        
        -- Company metrics and details
        properties_numberofemployees as number_of_employees,
        properties_hs_employee_range as employee_range,
        properties_annualrevenue as annual_revenue,
        properties_hs_annual_revenue_currency_code as annual_revenue_currency_code,
        properties_hs_revenue_range as revenue_range,
        properties_founded_year as founded_year,
        properties_is_public as is_public,
        properties_total_money_raised as total_money_raised,
        properties_total_revenue as total_revenue,
        
        -- Social media and web presence
        properties_twitterhandle as twitter_handle,
        properties_twitterbio as twitter_bio,
        properties_twitterfollowers as twitter_followers,
        properties_linkedinbio as linkedin_bio,
        properties_linkedin_company_page as linkedin_company_page,
        properties_hs_linkedin_handle as linkedin_handle,
        properties_facebook_company_page as facebook_company_page,
        properties_facebookfans as facebook_fans,
        properties_googleplus_page as googleplus_page,
        properties_web_technologies as web_technologies,
        
        -- HubSpot specific fields
        properties_hs_object_id as hubspot_object_id,
        properties_hubspot_owner_id as hubspot_owner_id,
        properties_hubspot_team_id as hubspot_team_id,
        properties_lifecyclestage as lifecycle_stage,
        properties_hs_lead_status as lead_status,
        properties_hs_pipeline as pipeline,
        properties_createdate as hubspot_create_date,
        properties_hs_createdate as hs_create_date,
        properties_hs_lastmodifieddate as last_modified_date,
        properties_hubspot_owner_assigneddate as owner_assigned_date,
        properties_closedate as close_date,
        properties_days_to_close as days_to_close,
        
        -- Engagement and activity metrics
        properties_hubspotscore as hubspot_score,
        properties_num_notes as number_of_notes,
        properties_num_contacted_notes as number_of_contacted_notes,
        properties_notes_last_updated as notes_last_updated,
        properties_notes_last_contacted as notes_last_contacted,
        properties_notes_next_activity_date as notes_next_activity_date,
        properties_hs_notes_last_activity as notes_last_activity,
        properties_hs_notes_next_activity as notes_next_activity,
        properties_hs_notes_next_activity_type as notes_next_activity_type,
        
        -- Deal and revenue tracking
        properties_num_associated_deals as number_of_associated_deals,
        properties_num_associated_contacts as number_of_associated_contacts,
        properties_recent_deal_amount as recent_deal_amount,
        properties_recent_deal_close_date as recent_deal_close_date,
        properties_first_deal_created_date as first_deal_created_date,
        properties_hs_total_deal_value as total_deal_value,
        properties_hs_num_open_deals as number_of_open_deals,
        
        -- Lifecycle stage timing
        properties_hs_time_in_lead as time_in_lead,
        properties_hs_time_in_other as time_in_other,
        properties_hs_time_in_customer as time_in_customer,
        properties_hs_time_in_evangelist as time_in_evangelist,
        properties_hs_time_in_subscriber as time_in_subscriber,
        properties_hs_time_in_opportunity as time_in_opportunity,
        properties_hs_time_in_salesqualifiedlead as time_in_sales_qualified_lead,
        properties_hs_time_in_marketingqualifiedlead as time_in_marketing_qualified_lead,
        
        -- Lifecycle stage dates
        properties_hs_date_entered_lead as date_entered_lead,
        properties_hs_date_exited_lead as date_exited_lead,
        properties_hs_date_entered_other as date_entered_other,
        properties_hs_date_exited_other as date_exited_other,
        properties_hs_date_entered_customer as date_entered_customer,
        properties_hs_date_exited_customer as date_exited_customer,
        properties_hs_date_entered_evangelist as date_entered_evangelist,
        properties_hs_date_exited_evangelist as date_exited_evangelist,
        properties_hs_date_entered_subscriber as date_entered_subscriber,
        properties_hs_date_exited_subscriber as date_exited_subscriber,
        properties_hs_date_entered_opportunity as date_entered_opportunity,
        properties_hs_date_exited_opportunity as date_exited_opportunity,
        properties_hs_date_entered_salesqualifiedlead as date_entered_sales_qualified_lead,
        properties_hs_date_exited_salesqualifiedlead as date_exited_sales_qualified_lead,
        properties_hs_date_entered_marketingqualifiedlead as date_entered_marketing_qualified_lead,
        properties_hs_date_exited_marketingqualifiedlead as date_exited_marketing_qualified_lead,
        
        -- Analytics and source tracking
        properties_hs_analytics_source as analytics_source,
        properties_hs_analytics_latest_source as analytics_latest_source,
        properties_hs_analytics_source_data_1 as analytics_source_data_1,
        properties_hs_analytics_source_data_2 as analytics_source_data_2,
        properties_hs_analytics_latest_source_data_1 as analytics_latest_source_data_1,
        properties_hs_analytics_latest_source_data_2 as analytics_latest_source_data_2,
        properties_hs_analytics_latest_source_timestamp as analytics_latest_source_timestamp,
        properties_hs_analytics_first_timestamp as analytics_first_timestamp,
        properties_hs_analytics_last_timestamp as analytics_last_timestamp,
        properties_hs_analytics_num_visits as analytics_visits,
        properties_hs_analytics_num_page_views as analytics_page_views,
        properties_hs_analytics_first_visit_timestamp as analytics_first_visit_timestamp,
        properties_hs_analytics_last_visit_timestamp as analytics_last_visit_timestamp,
        
        -- Conversion tracking
        properties_num_conversion_events as number_of_conversion_events,
        properties_first_conversion_date as first_conversion_date,
        properties_recent_conversion_date as recent_conversion_date,
        properties_first_conversion_event_name as first_conversion_event_name,
        properties_recent_conversion_event_name as recent_conversion_event_name,
        properties_first_contact_createdate as first_contact_create_date,
        
        -- Target account and enrichment
        properties_hs_target_account as is_target_account,
        properties_hs_is_target_account as is_target_account_flag,
        properties_hs_target_account_probability as target_account_probability,
        properties_hs_target_account_recommendation_state as target_account_recommendation_state,
        properties_hs_target_account_recommendation_snooze_time as target_account_recommendation_snooze_time,
        properties_hs_is_enriched as is_enriched,
        properties_hs_live_enrichment_deadline as live_enrichment_deadline,
        properties_hs_last_metered_enrichment_timestamp as last_metered_enrichment_timestamp,
        properties_hs_ideal_customer_profile as ideal_customer_profile,
        
        -- Company hierarchy and relationships
        properties_hs_parent_company_id as parent_company_id,
        properties_hs_num_child_companies as number_of_child_companies,
        properties_hs_num_decision_makers as number_of_decision_makers,
        properties_hs_num_contacts_with_buying_roles as number_of_contacts_with_buying_roles,
        properties_hs_additional_domains as additional_domains,
        
        -- Sales activity tracking
        properties_hs_last_sales_activity_date as last_sales_activity_date,
        properties_hs_last_sales_activity_type as last_sales_activity_type,
        properties_hs_last_sales_activity_timestamp as last_sales_activity_timestamp,
        properties_hs_sales_email_last_replied as sales_email_last_replied,
        properties_hs_last_booked_meeting_date as last_booked_meeting_date,
        properties_hs_last_logged_call_date as last_logged_call_date,
        properties_hs_last_logged_outgoing_email_date as last_logged_outgoing_email_date,
        properties_hs_latest_meeting_activity as latest_meeting_activity,
        properties_hs_last_open_task_date as last_open_task_date,
        
        -- Meeting engagement
        properties_engagements_last_meeting_booked as engagements_last_meeting_booked,
        properties_engagements_last_meeting_booked_medium as engagements_last_meeting_booked_medium,
        properties_engagements_last_meeting_booked_source as engagements_last_meeting_booked_source,
        properties_engagements_last_meeting_booked_campaign as engagements_last_meeting_booked_campaign,
        
        -- Campaign tracking
        properties_hs_analytics_first_touch_converting_campaign as analytics_first_touch_converting_campaign,
        properties_hs_analytics_last_touch_converting_campaign as analytics_last_touch_converting_campaign,
        
        -- HubSpot system fields
        properties_hs_object_source as object_source,
        properties_hs_object_source_id as object_source_id,
        properties_hs_object_source_label as object_source_label,
        properties_hs_object_source_user_id as object_source_user_id,
        properties_hs_object_source_detail_1 as object_source_detail_1,
        properties_hs_object_source_detail_2 as object_source_detail_2,
        properties_hs_object_source_detail_3 as object_source_detail_3,
        properties_hs_created_by_user_id as created_by_user_id,
        properties_hs_updated_by_user_id as updated_by_user_id,
        properties_hs_unique_creation_key as unique_creation_key,
        properties_hs_was_imported as was_imported,
        properties_hs_read_only as is_read_only,
        properties_hs_merged_object_ids as merged_object_ids,
        
        -- Team and ownership
        properties_hs_all_team_ids as all_team_ids,
        properties_hs_owning_teams as owning_teams,
        properties_hs_all_owner_ids as all_owner_ids,
        properties_hs_user_ids_of_all_owners as user_ids_of_all_owners,
        properties_hs_all_accessible_team_ids as all_accessible_team_ids,
        properties_hs_user_ids_of_all_notification_followers as user_ids_of_all_notification_followers,
        properties_hs_user_ids_of_all_notification_unfollowers as user_ids_of_all_notification_unfollowers,
        
        -- Additional properties
        properties_hs_keywords as keywords,
        properties_hs_logo_url as logo_url,
        properties_hs_task_label as task_label,
        properties_hs_quick_context as quick_context,
        properties_hs_num_blockers as number_of_blockers,
        properties_hs_pinned_engagement_id as pinned_engagement_id,
        properties_hs_avatar_filemanager_key as avatar_filemanager_key,
        properties_hs_research_agent_id as research_agent_id,
        properties_hs_research_agent_execution_id as research_agent_execution_id,
        properties_hs_latest_createdate_of_active_subscriptions as latest_createdate_of_active_subscriptions,
        
        -- Metadata columns
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id,
        _airbyte_meta

    from source
)

select * from renamed