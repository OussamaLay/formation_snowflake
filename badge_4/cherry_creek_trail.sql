
select *
from @trails_geojson
(file_format => ff_json);


select
    $1:sequence_1 as sequence_1,
    $1:trail_name::varchar as trail_name,
    $1:latitude as latitude,
    $1:longitude as longitude,
    $1:sequence_2 as sequence_2,
    $1:elevation as elevation 
from @trails_parquet
(file_format => ff_parquet)
order by sequence_1;


create or replace view CHERRY_CREEK_TRAIL as (
    select 
     $1:sequence_1 as point_id,
     $1:trail_name::varchar as trail_name,
     $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
     $1:longitude::number(11,8) as lat
    from @trails_parquet
    (file_format => ff_parquet)
    order by point_id
);

select top 100
    lng||' '||lat as coord_pair
    ,'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;


--To add a column, we have to replace the entire view
--changes to the original are shown in red
create or replace view cherry_creek_trail as
    select 
     $1:sequence_1 as point_id,
     $1:trail_name::varchar as trail_name,
     $1:latitude::number(11,8) as lng,
     $1:longitude::number(11,8) as lat,
     lng||' '||lat as coord_pair
    from @trails_parquet
    (file_format => ff_parquet)
    order by point_id;


select 
    'LINESTRING('||
    listagg(coord_pair, ',') 
    within group (order by point_id)
    ||')' as my_linestring
from cherry_creek_trail
where point_id <= 2450
group by trail_name;


select 
    'LINESTRING('||
    listagg(coord_pair, ',') 
    within group (order by point_id)
    ||')' as my_linestring
    ,st_length(TO_GEOGRAPHY(my_linestring)) as length_of_trail --this line is new! but it won't work!
from cherry_creek_trail
group by trail_name;



create or replace view DENVER_AREA_TRAILS as
    select
        $1:features[0]:properties:Name::string as feature_name
        ,$1:features[0]:geometry:coordinates::string as feature_coordinates
        ,$1:features[0]:geometry::string as geometry
        ,$1:features[0]:properties::string as feature_properties
        ,$1:crs:properties:name::string as specs
        ,$1 as whole_object
    from @trails_geojson (file_format => ff_json);


    
select
    feature_name,
    st_length(TO_GEOGRAPHY(whole_object)) as wo_length,
    st_length(TO_GEOGRAPHY(geometry)) as geom_length,
from denver_area_trails;



create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS(
	FEATURE_NAME,
	FEATURE_COORDINATES,
	GEOMETRY,
    TRAIL_LENGTH,
	FEATURE_PROPERTIES,
	SPECS,
	WHOLE_OBJECT
) as
    select
        $1:features[0]:properties:Name::string as feature_name
        ,$1:features[0]:geometry:coordinates::string as feature_coordinates
        ,$1:features[0]:geometry::string as geometry
        ,st_length(TO_GEOGRAPHY(geometry)) as trail_length
        ,$1:features[0]:properties::string as feature_properties
        ,$1:crs:properties:name::string as specs
        ,$1 as whole_object
    from @trails_geojson (file_format => ff_json);

    
select * from denver_area_trails;
select * from cherry_creek_trail;


--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create or replace view DENVER_AREA_TRAILS_2 as
    select 
        trail_name as feature_name
        ,'{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry
        ,st_length(to_geography(geometry))  as trail_length
    from cherry_creek_trail
    group by trail_name;


--Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;


--Add more GeoSpatial Calculations to get more GeoSpecial Information!
create or replace view trails_and_boundaries as
    select feature_name
    , to_geography(geometry) as my_linestring
    , st_xmin(my_linestring) as min_eastwest
    , st_xmax(my_linestring) as max_eastwest
    , st_ymin(my_linestring) as min_northsouth
    , st_ymax(my_linestring) as max_northsouth
    , trail_length
    from DENVER_AREA_TRAILS
    union all
    select feature_name
    , to_geography(geometry) as my_linestring
    , st_xmin(my_linestring) as min_eastwest
    , st_xmax(my_linestring) as max_eastwest
    , st_ymin(my_linestring) as min_northsouth
    , st_ymax(my_linestring) as max_northsouth
    , trail_length
    from DENVER_AREA_TRAILS_2;



select 'POLYGON(('|| 
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from trails_and_boundaries;



////////////////////////////
////////////////////////////


create or replace  external table T_CHERRY_CREEK_TRAIL(
	my_filename varchar(100) as (metadata$filename::varchar(100))
) 
location= @EXTERNAL_AWS_DLKW
auto_refresh = true
file_format = (type = parquet);

select * from T_CHERRY_CREEK_TRAIL;

////////////////////////////
////////////////////////////

create secure materialized view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR,
    DISTANCE_TO_MELANIES
) as
select 
 value:sequence_1 as point_id,
 value:trail_name::varchar as trail_name,
 value:latitude::number(11,8) as lng,
 value:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair,
 locations.distance_to_mc(lng,lat) as distance_to_melanies
from t_cherry_creek_trail;

////////////////////////////
////////////////////////////
use role ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'iceberg-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://uni-dlkw-iceberg'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::321463406630:role/dlkw_iceberg_role'
            STORAGE_AWS_EXTERNAL_ID = 'dlkw_iceberg_id'
         )
      );

DESC EXTERNAL VOLUME iceberg_external_volume;

create database my_iceberg_db
 catalog = 'SNOWFLAKE'
 external_volume = 'iceberg_external_volume';


set table_name = 'CCT_'||current_account();

create iceberg table identifier($table_name) (
    point_id number(10,0)
    , trail_name string
    , coord_pair string
    , distance_to_melanies decimal(20,10)
    , user_name string
)
  BASE_LOCATION = $table_name
  AS SELECT top 100
    point_id
    , trail_name
    , coord_pair
    , distance_to_melanies
    , current_user()
  FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL;


select * from identifier($table_name); 


update identifier($table_name)
set user_name = 'I am amazing!!'
where point_id = 1;
