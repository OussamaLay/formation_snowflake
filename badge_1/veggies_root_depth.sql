--Be sure to set your context menus
create or replace table ROOT_DEPTH (
   ROOT_DEPTH_ID number(1), 
   ROOT_DEPTH_CODE text(1), 
   ROOT_DEPTH_NAME text(7), 
   UNIT_OF_MEASURE text(2),
   RANGE_MIN number(2),
   RANGE_MAX number(2)
   );

insert into root_depth 
values
(
    3,
    'D',
    'Deep',
    'cm',
    60,
    90
);

select * 
from garden_plants.information_schema.schemata;

SELECT * 
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');


select count(*) as schemas_found, '3' as schemas_expected 
from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');


create table garden_plants.veggies.vegetable_details
(
plant_name varchar(25)
, root_depth_code varchar(1)    
);


delete from VEGETABLE_DETAILS
where PLANT_NAME='Spinach'
and root_depth_code='D';


select * from VEGETABLE_DETAILS;


-- Create a Table for Soil Types
create or replace table vegetable_details_soil_type
( plant_name varchar(25)
 ,soil_type number(1,0)
);


-- Create a File Format
create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    type = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    field_delimiter = '|' --pipes as column separators
    skip_header = 1 --one header row to skip
    ;

-- A Copy Into Statement You Can Run
copy into vegetable_details_soil_type
from @util_db.public.my_internal_stage
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW );



-- Create Another File Format
create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = ',' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;



-- Create Another File Format
create file format garden_plants.veggies.L9_CHALLENGE_FF 
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = '\t' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    --FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;



--The data in the file, with no FILE FORMAT specified
select $1
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv;

--Same file but with one of the file formats we created earlier  
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

--Same file but with the other file format we created earlier
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );

--Same file but with the other file format we created earlier (THE RIGHT ONE)
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.L9_CHALLENGE_FF );


-- Create a Soil Type Look Up Table
create or replace table LU_SOIL_TYPE(
SOIL_TYPE_ID number,	
SOIL_TYPE varchar(15),
SOIL_DESCRIPTION varchar(75)
 );

-- A Copy Into Statement You Can Run
copy into LU_SOIL_TYPE
from @util_db.public.my_internal_stage
files = ( 'LU_SOIL_TYPE.tsv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.L9_CHALLENGE_FF );


---
--- loading veg_plant_height.csv into VEGETABLE_DETAILS_PLANT_HEIGHT
---

-- Create a VEGETABLE PLANT HEIGHT Table
create or replace table VEGETABLE_DETAILS_PLANT_HEIGHT (
plant_name varchar(20),	
UOM varchar(1),
Low_End_of_Range number,
High_End_of_Range number
 );

-- Testing the exact file format
select $1, $2, $3, $4
from @util_db.public.my_internal_stage/veg_plant_height.csv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW );

-- loading into the table
copy into VEGETABLE_DETAILS_PLANT_HEIGHT
from @util_db.public.my_internal_stage
files = ( 'veg_plant_height.csv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW );


