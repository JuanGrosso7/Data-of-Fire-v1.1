{{ config(
    materialized='incremental',
    unique_key='id'
) }}

WITH transformed_data AS (
    SELECT
        id::VARCHAR AS id,
        CAST({{ clean_null('incident_number') }} AS INTEGER) AS incident_number,
        CAST({{ clean_null('exposure_number') }} AS INTEGER) AS exposure_number,
        {{ clean_null('address') }} AS address,
        CAST({{ clean_null('incident_date') }} AS TIMESTAMP) AS incident_date,
        {{ clean_null('call_number') }}::VARCHAR AS call_number,
        CAST({{ clean_null('alarm_dttm') }} AS TIMESTAMP) AS alarm_dttm,
        CAST({{ clean_null('arrival_dttm') }} AS TIMESTAMP) AS arrival_dttm,
        CAST({{ clean_null('close_dttm') }} AS TIMESTAMP) AS close_dttm,
        {{ clean_null('city') }} AS city,
        {{ clean_null('zipcode') }}::VARCHAR AS zipcode,
        {{ clean_null('battalion') }} AS battalion,
        {{ clean_null('station_area') }} AS station_area,
        {{ clean_null('box') }} AS box,
        CAST({{ clean_null('suppression_units') }} AS INTEGER) AS suppression_units,
        CAST({{ clean_null('suppression_personnel') }} AS INTEGER) AS suppression_personnel,
        CAST({{ clean_null('ems_units') }} AS INTEGER) AS ems_units,
        CAST({{ clean_null('ems_personnel') }} AS INTEGER) AS ems_personnel,
        CAST({{ clean_null('other_units') }} AS INTEGER) AS other_units,
        CAST({{ clean_null('other_personnel') }} AS INTEGER) AS other_personnel,
        {{ clean_null('first_unit_on_scene') }} AS first_unit_on_scene,
        CAST({{ clean_null('estimated_property_loss') }} AS FLOAT) AS estimated_property_loss,
        CAST({{ clean_null('estimated_contents_loss') }} AS FLOAT) AS estimated_contents_loss,
        CAST({{ clean_null('fire_fatalities') }} AS INTEGER) AS fire_fatalities,
        CAST({{ clean_null('fire_injuries') }} AS INTEGER) AS fire_injuries,
        CAST({{ clean_null('civilian_fatalities') }} AS INTEGER) AS civilian_fatalities,
        CAST({{ clean_null('civilian_injuries') }} AS INTEGER) AS civilian_injuries,
        CAST({{ clean_null('number_of_alarms') }} AS INTEGER) AS number_of_alarms,
        {{ clean_null('primary_situation') }} AS primary_situation,
        {{ clean_null('mutual_aid') }} AS mutual_aid,
        {{ clean_null('action_taken_primary') }} AS action_taken_primary,
        {{ clean_null('action_taken_secondary') }} AS action_taken_secondary,
        {{ clean_null('action_taken_other') }} AS action_taken_other,
        {{ clean_null('detector_alerted_occupants') }} AS detector_alerted_occupants,
        {{ clean_null('property_use') }} AS property_use,
        {{ clean_null('area_of_fire_origin') }} AS area_of_fire_origin,
        {{ clean_null('ignition_cause') }} AS ignition_cause,
        {{ clean_null('ignition_factor_primary') }} AS ignition_factor_primary,
        {{ clean_null('ignition_factor_secondary') }} AS ignition_factor_secondary,
        {{ clean_null('heat_source') }} AS heat_source,
        {{ clean_null('item_first_ignited') }} AS item_first_ignited,
        {{ clean_null('human_factors_associated_with_ignition') }} AS human_factors_associated_with_ignition,
        {{ clean_null('structure_type') }} AS structure_type,
        {{ clean_null('structure_status') }} AS structure_status,
        {{ clean_null('floor_of_fire_origin') }} AS floor_of_fire_origin,
        {{ clean_null('fire_spread') }} AS fire_spread,
        {{ clean_null('no_flame_spread') }} AS no_flame_spread,
        {{ clean_null('number_of_floors_with_minimum_damage') }} AS number_of_floors_with_minimum_damage,
        {{ clean_null('number_of_floors_with_significant_damage') }} AS number_of_floors_with_significant_damage,
        {{ clean_null('number_of_floors_with_heavy_damage') }} AS number_of_floors_with_heavy_damage,
        {{ clean_null('number_of_floors_with_extreme_damage') }} AS number_of_floors_with_extreme_damage,
        {{ clean_null('detectors_present') }} AS detectors_present,
        {{ clean_null('detector_type') }} AS detector_type,
        {{ clean_null('detector_operation') }} AS detector_operation,
        {{ clean_null('detector_effectiveness') }} AS detector_effectiveness,
        {{ clean_null('detector_failure_reason') }} AS detector_failure_reason,
        {{ clean_null('automatic_extinguishing_system_present') }} AS automatic_extinguishing_system_present,
        {{ clean_null('automatic_extinguishing_system_type') }} AS automatic_extinguishing_sytem_type,
        {{ clean_null('automatic_extinguishing_system_performance') }} AS automatic_extinguishing_sytem_perfomance,
        {{ clean_null('automatic_extinguishing_system_failure_reason') }} AS automatic_extinguishing_sytem_failure_reason,
        CAST({{ clean_null('number_of_sprinkler_heads_operating') }} AS INTEGER) AS number_of_sprinkler_heads_operating,
        {{ clean_null('supervisor_district') }} AS supervisor_district,
        {{ clean_null('neighborhood_district') }} AS neighborhood_district,
        CASE 
            WHEN {{ clean_null('point') }} IS NULL THEN NULL
            ELSE ST_PointFromText({{ clean_null('point') }}, 4326) 
        END AS point,
        CAST({{ clean_null('data_as_of') }} AS TIMESTAMP) AS data_as_of,
        CAST({{ clean_null('data_loaded_at') }} AS TIMESTAMP) AS data_loaded_at
    FROM public.staging_fire_incidents
)

SELECT * FROM transformed_data

{% if is_incremental() %}
WHERE id::VARCHAR NOT IN (SELECT id::VARCHAR FROM {{ this }})
{% endif %}
