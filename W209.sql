SELECT fact.household_id||'-'||fact.person_id AS id,
       person.gender_cd AS gender,
       person.age_cd AS age,
       household.household_income_amt_desc AS household_income,
       household.county_size_desc AS county_size,
       household.geographic_territory_desc AS geographic_territory,
       person.education_desc AS education,
       person.nielsen_occupation_desc AS occupation,
       fact.broadcast_dt AS broadcast_date,
       fact.view_dt AS view_date,
       fact.view_dt - fact.broadcast_dt AS delayDays,
       asset.nielsen_amrld_network_cd AS network,
       fact.nielsen_amrld_network_type_desc AS network_type,
       asset.nielsen_amrld_program_nm AS program_name,
       asset.nielsen_amrld_episode_nm AS episode_name,
       asset.program_type_summary_nm AS program_type,
       asset.nhi_nhih_program_type_nm AS detail_program_type,
       asset.reported_duration_mins AS program_duration,
       fact.viewing_mins AS viewing_mins,
       1.0*fact.viewing_mins/asset.reported_duration_mins AS perc_viewed
FROM edw.fact_nielsen_amrld_viewership AS fact
  JOIN edw.dim_nielsen_amrld_video_asset_master AS asset
    ON asset.nielsen_amrld_video_asset_master_key = fact.nielsen_amrld_video_asset_master_key
  JOIN edw.dim_nielsen_amrld_person_classification_weight AS person
    ON person.person_key = fact.person_key
  JOIN edw.dim_nielsen_amrld_household_classification_weight AS household
    ON household.household_key = fact.household_key
WHERE person.age_cd >= 18 AND person.age_cd <= 49 AND
      fact.visitor_status_cd <> 'S' AND
      -- asset.repeat_ind = 0 AND
      (asset.nielsen_amrld_network_type_desc = 'Broadcast Network' OR
      asset.nielsen_amrld_network_type_desc = 'Cable Network') AND
      viewing_mins >= 6 AND
      fact.broadcast_dt >= '2018-05-01' AND
      fact.broadcast_dt <= '2018-06-30' AND
      asset.nielsen_amrld_reported_end_tm <= 2400
ORDER BY id, view_date, network, program_name, episode_name
;


SELECT agg.broadcast_dt AS date,
       agg.demographic_category_desc AS demo,
       agg.data_stream_nm AS data_stream,
       agg.nielsen_amrld_program_nm AS program_name,
       asset.nielsen_amrld_daypart_nm AS daypart,
       asset.nielsen_amrld_network_cd AS network,
       asset.nielsen_amrld_network_type_desc AS network_type,
       agg.weighted_viewing_mins AS viewing_minutes,
       agg.total_nielsen_viewers_weighted AS viewers,
       agg.weighted_viewing_mins/agg.reported_duration_mins/1000 AS average_audience
FROM edw.agg_nielsen_amrld_program_demographic_rating AS agg
  JOIN edw.dim_nielsen_amrld_video_asset_master AS asset
    ON asset.nielsen_amrld_video_asset_master_key = agg.nielsen_amrld_video_asset_master_key
WHERE agg.broadcast_dt >= '2018-05-01' AND
      agg.broadcast_dt <= '2018-06-30' AND
      asset.repeat_ind = 0 AND
      agg.market_break_desc = 'TOTAL COMPOSITE' AND
      (
        agg.demographic_category_desc = 'PERSONS 18 - 34' OR
        agg.demographic_category_desc = 'PERSONS 18 - 49' OR
        agg.demographic_category_desc = 'MALES 18 - 34' OR
        agg.demographic_category_desc = 'MALES 18 - 49' OR
        agg.demographic_category_desc = 'FEMALES 18 - 34' OR
        agg.demographic_category_desc = 'FEMALES 18 - 49'
      ) AND
      agg.data_stream_nm = 'program live+7d' AND
      (
        asset.nielsen_amrld_network_type_desc = 'Broadcast Network' OR
        asset.nielsen_amrld_network_type_desc = 'Cable Network'
      )
;

SELECT fact.household_id||'-'||fact.person_id AS id,
       person.gender_cd AS gender,
       person.age_cd AS age,
       fact.broadcast_dt AS broadcast_date,
       fact.view_dt AS view_date,
       asset.nielsen_amrld_network_cd AS network,
       fact.nielsen_amrld_network_type_desc AS network_type,
       asset.nielsen_amrld_program_nm AS program_name,
       asset.nielsen_amrld_episode_nm AS episode_name,
       asset.repeat_ind AS repeat_flag,
       asset.nielsen_amrld_reported_start_tm AS program_start_time,
       fact.program_viewing_start_minute AS viewing_start_min,
       fact.program_viewing_end_minute AS viewing_end_min,
       fact.viewing_mins AS viewing_mins
FROM edw.fact_nielsen_amrld_viewership AS fact
  JOIN edw.dim_nielsen_amrld_video_asset_master AS asset
    ON asset.nielsen_amrld_video_asset_master_key = fact.nielsen_amrld_video_asset_master_key
  JOIN edw.dim_nielsen_amrld_person_classification_weight AS person
    ON person.person_key = fact.person_key
WHERE person.age_cd >= 18 AND person.age_cd <= 49 AND
      fact.visitor_status_cd <> 'S' AND
      asset.repeat_ind = 0 AND
      fact.broadcast_dt >= '2018-06-01' AND
      fact.broadcast_dt <= '2018-07-31' AND
      asset.nielsen_amrld_reported_end_tm <= 2400
ORDER BY id, broadcast_date, program_start_time, viewing_start_min
;

SELECT fact.broadcast_dt, view_dt, min_play_delay_mins/60.0/24.0 AS calcDelayDays, view_dt - fact.broadcast_dt AS delayDays, asset.nielsen_amrld_program_nm
FROM edw.fact_nielsen_amrld_viewership AS fact
       JOIN edw.dim_nielsen_amrld_video_asset_master AS asset
              ON asset.nielsen_amrld_video_asset_master_key = fact.nielsen_amrld_video_asset_master_key
WHERE tm_shifted_viewing_key = 3 AND
      (min_play_delay_mins/60.0/24.0) > 7 AND
      (fact.age >= 18 AND fact.age <= 49) AND
      repeat_ind = FALSE AND
      (fact.nielsen_amrld_network_type_desc = 'Broadcast Network' OR
      fact.nielsen_amrld_network_type_desc = 'Cable Network')
;