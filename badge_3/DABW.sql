create or replace table SMOOTHIES.PUBLIC.FRUIT_OPTIONS
( FRUIT_ID number
 , FRUIT_NAME varchar(50)
);

create file format smoothies.public.two_headerrow_pct_delim
   type = CSV,
   skip_header = 2,   
   field_delimiter = '%',
   trim_space = TRUE
;

SELECT $2 as FRUIT_ID, $1 as FRUIT_NAME
FROM @SMOOTHIES.PUBLIC.MY_UPLOADED_FILES/fruits_available_for_smoothies.txt
(FILE_FORMAT => two_headerrow_pct_delim);

COPY INTO smoothies.public.fruit_options
from (SELECT $2 as FRUIT_ID, $1 as FRUIT_NAME
      FROM @SMOOTHIES.PUBLIC.MY_UPLOADED_FILES/fruits_available_for_smoothies.txt)
file_format = (format_name = smoothies.public.two_headerrow_pct_delim)
on_error = abort_statement
purge = true;


///---

create or replace table smoothies.public.orders (
    ingredients varchar(200)
);

insert into smoothies.public.orders(ingredients) values ('Apples Blueberries Cantaloupe Dragon Fruit Elderberries ');

select * from orders;

--truncate table smoothies.public.orders;

alter table smoothies.public.orders add column name_on_order varchar(100);

ALTER TABLE smoothies.public.orders ADD COLUMN ORDER_FILLED BOOLEAN DEFAULT FALSE;

update smoothies.public.orders
set order_filled = true
where name_on_order is null;

alter table SMOOTHIES.PUBLIC.ORDERS 
add column order_uid integer --adds the column
default smoothies.public.order_seq.nextval  --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column


///---


create or replace table smoothies.public.orders (
       order_uid integer default smoothies.public.order_seq.nextval,
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       constraint order_uid unique (order_uid),
       order_ts timestamp_ltz default current_timestamp()
);


///---


alter table smoothies.public.FRUIT_OPTIONS add column SEARCH_ON varchar(100);

select * from FRUIT_OPTIONS;

update smoothies.public.FRUIT_OPTIONS
set SEARCH_ON = FRUIT_NAME;

-- Apple
update smoothies.public.FRUIT_OPTIONS
set SEARCH_ON = 'Apple'
where FRUIT_NAME='Apples';

-- Blueberries
update smoothies.public.FRUIT_OPTIONS
set SEARCH_ON = 'Blueberry'
where FRUIT_NAME='Blueberries';

-- Jack Fruit
update smoothies.public.FRUIT_OPTIONS
set SEARCH_ON = 'Jackfruit'
where FRUIT_NAME='Jack Fruit';

-- Raspberry
update smoothies.public.FRUIT_OPTIONS
set SEARCH_ON = 'Raspberry'
where FRUIT_NAME='Raspberries';

-- Strawberry
update smoothies.public.FRUIT_OPTIONS
set SEARCH_ON = 'Strawberry'
where FRUIT_NAME='Strawberries';
