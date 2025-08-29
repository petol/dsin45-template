{{ config(materialized='view') }}

with source as (
    select * from {{ source('lake', 'deals') }}
),

renamed as (
    select
        id as deal_id,
        archived,
        contacts,
        companies,
        "createdAt" as created_at,
        "updatedAt" as updated_at,
        line_items,
        
        -- Core deal information
        properties_dealname as deal_name,
        properties_dealtype as deal_type,
        properties_pipeline as pipeline,
        properties_dealstage as deal_stage,
        properties_description as description,
        
        -- Financial information
        properties_amount as amount,
        properties_hs_acv as annual_contract_value,
        properties_hs_arr as annual_recurring_revenue,
        properties_hs_mrr as monthly_recurring_revenue,
        properties_hs_tcv as total_contract_value,
        properties_hs_closed_amount as closed_amount,
        properties_hs_forecast_amount as forecast_amount,
        properties_hs_predicted_amount as predicted_amount,
        properties_hs_projected_amount as projected_amount,
        properties_amount_in_home_currency as amount_in_home_currency,
        properties_hs_closed_amount_in_home_currency as closed_amount_in_home_currency,
        properties_hs_open_amount_in_home_currency as open_amount_in_home_currency,
        properties_hs_predicted_amount_in_home_currency as predicted_amount_in_home_currency,
        properties_hs_projected_amount_in_home_currency as projected_amount_in_home_currency,
        properties_hs_exchange_rate as exchange_rate,
        
        -- Date information
        properties_closedate as close_date,
        properties_createdate as hubspot_create_date,
        properties_hs_createdate as hs_create_date,
        properties_hs_closed_won_date as closed_won_date,
        properties_hs_lastmodifieddate as last_modified_date,
        properties_hubspot_owner_assigneddate as owner_assigned_date,
        
        -- Deal status and stage information
        properties_hs_is_closed as is_closed,
        properties_hs_is_closed_won as is_closed_won,
        properties_hs_is_closed_lost as is_closed_lost,
        properties_hs_is_open_count as is_open_count,
        properties_hs_is_closed_count as is_closed_count,
        properties_hs_closed_won_count as closed_won_count,
        properties_hs_deal_stage_probability as deal_stage_probability,
        properties_hs_deal_stage_probability_shadow as deal_stage_probability_shadow,
        properties_hs_forecast_probability as forecast_probability,
        properties_hs_is_in_first_deal_stage as is_in_first_deal_stage,
        properties_closed_won_reason as closed_won_reason,
        properties_closed_lost_reason as closed_lost_reason,
        
        -- Activity and timing metrics
        properties_days_to_close as days_to_close,
        properties_hs_days_to_close_raw as days_to_close_raw,
        properties_num_notes as number_of_notes,
        properties_num_contacted_notes as number_of_contacted_notes,
        properties_num_associated_contacts as number_of_associated_contacts,
        properties_notes_last_updated as notes_last_updated,
        properties_notes_last_contacted as notes_last_contacted,
        properties_notes_next_activity_date as notes_next_activity_date,
        
        -- HubSpot specific fields
        properties_hs_object_id as hubspot_object_id,
        properties_hubspot_owner_id as hubspot_owner_id,
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
        
        -- Campaign and marketing
        properties_hs_campaign as campaign,
        properties_hs_analytics_source as analytics_source,
        properties_hs_analytics_latest_source as analytics_latest_source,
        properties_hs_analytics_source_data_1 as analytics_source_data_1,
        properties_hs_analytics_source_data_2 as analytics_source_data_2,
        properties_hs_analytics_latest_source_data_1 as analytics_latest_source_data_1,
        properties_hs_analytics_latest_source_data_2 as analytics_latest_source_data_2,
        properties_hs_analytics_latest_source_timestamp as analytics_latest_source_timestamp,
        
        -- Team and ownership
        properties_hubspot_team_id as hubspot_team_id,
        properties_hs_all_team_ids as all_team_ids,
        properties_hs_owning_teams as owning_teams,
        properties_hs_all_owner_ids as all_owner_ids,
        properties_hs_user_ids_of_all_owners as user_ids_of_all_owners,
        properties_hs_all_accessible_team_ids as all_accessible_team_ids,
        properties_hs_all_collaborator_owner_ids as all_collaborator_owner_ids,
        
        -- Additional properties
        properties_hs_priority as priority,
        properties_hs_next_step as next_step,
        properties_hs_next_step_updated_at as next_step_updated_at,
        properties_hs_likelihood_to_close as likelihood_to_close,
        properties_hs_manual_forecast_category as manual_forecast_category,
        properties_hs_latest_approval_status as latest_approval_status,
        properties_hs_primary_associated_company as primary_associated_company,
        
        -- Engagement metrics
        properties_hs_num_of_associated_line_items as number_of_associated_line_items,
        properties_hs_number_of_call_engagements as number_of_call_engagements,
        properties_hs_number_of_inbound_calls as number_of_inbound_calls,
        properties_hs_number_of_outbound_calls as number_of_outbound_calls,
        properties_hs_number_of_scheduled_meetings as number_of_scheduled_meetings,
        properties_hs_average_call_duration as average_call_duration,
        properties_hs_latest_meeting_activity as latest_meeting_activity,
        properties_hs_next_meeting_id as next_meeting_id,
        properties_hs_next_meeting_name as next_meeting_name,
        properties_hs_next_meeting_start_time as next_meeting_start_time,
        
        -- Email engagement
        properties_hs_sales_email_last_replied as sales_email_last_replied,
        properties_hs_latest_sales_email_open_date as latest_sales_email_open_date,
        properties_hs_latest_sales_email_click_date as latest_sales_email_click_date,
        properties_hs_latest_sales_email_reply_date as latest_sales_email_reply_date,
        properties_hs_latest_marketing_email_open_date as latest_marketing_email_open_date,
        properties_hs_latest_marketing_email_click_date as latest_marketing_email_click_date,
        properties_hs_latest_marketing_email_reply_date as latest_marketing_email_reply_date,
        
        -- Meeting engagement
        properties_engagements_last_meeting_booked as engagements_last_meeting_booked,
        properties_engagements_last_meeting_booked_medium as engagements_last_meeting_booked_medium,
        properties_engagements_last_meeting_booked_source as engagements_last_meeting_booked_source,
        properties_engagements_last_meeting_booked_campaign as engagements_last_meeting_booked_campaign,
        
        -- Metadata columns
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id,
        _airbyte_meta

    from source
)

select * from renamed