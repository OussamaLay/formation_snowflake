create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    schedule='10 minute'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	as copy into ags_game_audience.raw.PL_GAME_LOGS
    from @ags_game_audience.raw.uni_kishore_pipeline
    file_format = (format_name=FF_JSON_LOGS);


///////////////////////////////////////////////


create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    --after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    SCHEDULE = '5 Minutes'
	as 
    MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
    USING (
        SELECT pl.ip_address
            , pl.user_login as GAMER_NAME
            , pl.user_event as GAME_EVENT_NAME
            , pl.datetime_iso8601 as GAME_EVENT_UTC
            , city
            , region
            , country
            , timezone as GAMER_LTZ_NAME
            , CONVERT_TIMEZONE( 'UTC' , timezone , pl.datetime_iso8601 ) as GAME_EVENT_LTZ 
            , DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
            , tod.TOD_NAME
        from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS pl
        JOIN IPINFO_GEOLOC.demo.location loc 
            ON IPINFO_GEOLOC.public.TO_JOIN_KEY(pl.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.public.TO_INT(pl.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.time_of_day_lu tod
            ON HOUR(GAME_EVENT_LTZ) = tod.hour
        ) r --we'll put our fancy select here
    ON r.GAMER_NAME = e.GAMER_NAME
    and r.game_event_utc = e.game_event_utc
    and r.game_event_name = e.game_event_name
    WHEN NOT MATCHED THEN
    insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
            GAME_EVENT_UTC, CITY, REGION,
            COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ,
            DOW_NAME, TOD_NAME) --list of columns
    values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
            GAME_EVENT_UTC, CITY, REGION,
            COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ,
            DOW_NAME, TOD_NAME) --list of columns (but we can mark as coming from the r select)
    ;


///////////////////////////////////////////////


CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);


///////////////////////////////////////////////


--Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
WHEN
    system$stream_has_data('ed_cdc_stream')
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;
