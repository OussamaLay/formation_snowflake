alter user KISHOREK set default_role = 'SYSADMIN';
alter user KISHOREK set default_warehouse = 'COMPUTE_WH';
alter user KISHOREK set default_namespace = 'UTIL_DB.PUBLIC';


list @uni_kishore/kickoff;


create or replace file format FF_JSON_LOGS
type=JSON,
strip_outer_array = true;

select $1
from @uni_kishore/kickoff
(file_format => FF_JSON_LOGS);

copy into ags_game_audience.raw.game_logs
from @uni_kishore/kickoff
file_format = (format_name=FF_JSON_LOGS);

create or replace view logs as
    select
        RAW_LOG:agent::TEXT as agent
        ,RAW_LOG:user_event::TEXT as user_event
        ,RAW_LOG:user_login::TEXT as user_login
        ,RAW_LOG:datetime_iso8601::TIMESTAMP as datetime_iso8601
        ,*
    from game_logs;

select * from logs;


/////////////////////////////



--what time zone is your account(and/or session) currently set to? Is it -0700?
select current_timestamp();

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';


/////////////////////////////////


list @uni_kishore;

select $1
from @uni_kishore/updated_feed
(file_format => FF_JSON_LOGS);

copy into ags_game_audience.raw.game_logs
from @uni_kishore/updated_feed
file_format = (format_name=FF_JSON_LOGS);

create or replace view logs as
    select
        RAW_LOG:agent::TEXT as agent
        ,RAW_LOG:user_event::TEXT as user_event
        ,RAW_LOG:user_login::TEXT as user_login
        ,RAW_LOG:datetime_iso8601::TIMESTAMP as datetime_iso8601
        ,*
    from game_logs;


select
    raw_log
    ,RAW_LOG:agent::TEXT as agent
    ,RAW_LOG:ip_address::TEXT as ip_address
    ,RAW_LOG:user_event::TEXT as user_event
    ,RAW_LOG:user_login::TEXT as user_login
    ,RAW_LOG:datetime_iso8601::TIMESTAMP as datetime_iso8601
from game_logs;


--looking for empty AGENT column
select * from logs
where agent is null;

--looking for non-empty IP_ADDRESS column
select 
    RAW_LOG:ip_address::text as IP_ADDRESS
    ,*
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

create or replace view logs as
    select 
        RAW_LOG:ip_address::text as IP_ADDRESS
        ,RAW_LOG:user_event::TEXT as user_event
        ,RAW_LOG:user_login::TEXT as user_login
        ,RAW_LOG:datetime_iso8601::TIMESTAMP as datetime_iso8601
        ,raw_log
    from game_logs
    where RAW_LOG:ip_address::text is not null;


select * from logs
WHERE USER_LOGIN like '%prajina%';

select parse_ip('100.41.16.160','inet');


/////////////////////////////////


--Look up Kishore and Prajina's Time Zone in the IPInfo share using his headset's IP Address with the PARSE_IP function.
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
create table ags_game_audience.enhanced.logs_enhanced as(
    SELECT logs.ip_address
        , logs.user_login as GAMER_NAME
        , logs.user_event as GAME_EVENT_NAME
        , logs.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC' , timezone , logs.datetime_iso8601 ) as GAME_EVENT_LTZ 
        , DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
        , TOD_NAME
    from AGS_GAME_AUDIENCE.RAW.LOGS logs
    JOIN IPINFO_GEOLOC.demo.location loc 
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN AGS_GAME_AUDIENCE.RAW.time_of_day_lu tod
        ON HOUR(GAME_EVENT_LTZ) = tod.hour
);


--a Look Up table to convert from hour number to "time of day name"
create table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

--insert statement to add all 24 rows to the table
insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

--Check your table to see if you loaded it properly
select tod_name, listagg(hour,',') 
from time_of_day_lu
group by tod_name;


--clone the table to save this version as a backup (BU stands for Back Up)
create table ags_game_audience.enhanced.LOGS_ENHANCED_BU 
clone ags_game_audience.enhanced.LOGS_ENHANCED;


MERGE INTO ENHANCED.LOGS_ENHANCED e
USING RAW.LOGS r
ON r.user_login = e.GAMER_NAME
AND r.datetime_iso8601 = e.GAME_EVENT_UTC
AND r.user_event = e.GAME_EVENT_NAME
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey I updated matching rows!';

MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (
    SELECT logs.ip_address
        , logs.user_login as GAMER_NAME
        , logs.user_event as GAME_EVENT_NAME
        , logs.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC' , timezone , logs.datetime_iso8601 ) as GAME_EVENT_LTZ 
        , DAYNAME(GAME_EVENT_LTZ) as DOW_NAME
        , TOD_NAME
    from AGS_GAME_AUDIENCE.RAW.LOGS logs
    JOIN IPINFO_GEOLOC.demo.location loc 
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
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


select * from ENHANCED.LOGS_ENHANCED;


//////////////////////////////////////////////

use role accountadmin;
--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
grant execute task on account to role SYSADMIN;

use role sysadmin; 

--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task 
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;


/////////////////////////////////////////////

--make a note of how many rows you have in the table
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added (if any! HINT: Probably NONE!)
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


/////////////////////////////////////////////


create or replace TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS  (
	RAW_LOG VARIANT
);

copy into ags_game_audience.raw.PL_GAME_LOGS
from @ags_game_audience.raw.uni_kishore_pipeline
file_format = (format_name=FF_JSON_LOGS);

execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;


/////////////////////////////////////////////


create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	IP_ADDRESS,
	USER_EVENT,
	USER_LOGIN,
	DATETIME_ISO8601,
	RAW_LOG
) as
    select 
        RAW_LOG:ip_address::text as IP_ADDRESS
        ,RAW_LOG:user_event::TEXT as user_event
        ,RAW_LOG:user_login::TEXT as user_login
        ,RAW_LOG:datetime_iso8601::TIMESTAMP as datetime_iso8601
        ,raw_log
    from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    where RAW_LOG:ip_address::text is not null;


execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

truncate table PL_GAME_LOGS;


//////////////////////////////////////////

--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switch back to sysadmin
use role sysadmin;


////////////////////////////////////////////


create or replace table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS as (
    SELECT
    METADATA$FILENAME as log_file_name --new metadata column
    , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
    , current_timestamp(0) as load_ltz --new local time of load
    , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
    , get($1,'user_event')::text as USER_EVENT
    , get($1,'user_login')::text as USER_LOGIN
    , get($1,'ip_address')::text as IP_ADDRESS    
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
    (file_format => 'ff_json_logs')
);


--truncate the table rows that were input during the CTAS, if you used a CTAS and didn't recreate it with shorter VARCHAR fields
truncate table ED_PIPELINE_LOGS;

--reload the table using your COPY INTO
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


////////////////////////////////////////////


truncate table ENHANCED.LOGS_ENHANCED;

--Use this command if your Snowpipe seems like it is stalled out:
ALTER PIPE ags_game_audience.raw.PIPE_GET_NEW_FILES REFRESH;

--Use this command if you want to check that your pipe is running:
select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));


////////////////////////////////////////////


--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--look at the stream you created
show streams;


--query the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--check to see if any changes are pending (expect FALSE the first time you run it)
--after the Snowpipe loads a new file, expect to see TRUE
select system$stream_has_data('ed_cdc_stream');

--if your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('PIPE_GET_NEW_FILES');

--if you need to pause or unpause your pipe
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = false;




 
--process the stream by using the rows in a merge 
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
 
--Did all the rows from the stream disappear? 
select * 
from ags_game_audience.raw.ed_cdc_stream;


//////////////////////////////////////////////////


--the ListAgg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;


select GAMER_NAME
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;
