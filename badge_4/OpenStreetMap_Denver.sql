// Give me the length of a Way
SELECT
ID,
ST_LENGTH(COORDINATES) AS LENGTH
FROM DENVER.V_OSM_DEN_WAY;

// List the number of nodes in a Way
SELECT
ID,
ST_NPOINTS(COORDINATES) AS NUM_OF_NODES
FROM DENVER.V_OSM_DEN_WAY;

// Give me the distance between two Ways
SELECT
 A.ID AS ID_1,
 B.ID AS ID_2,
 ST_DISTANCE(A.COORDINATES, B.COORDINATES) AS DISTANCE
FROM (SELECT
 ID,
 COORDINATES
FROM DENVER.V_OSM_DEN_WAY
WHERE ID = 705859567) AS A
INNER JOIN (SELECT
 ID,
 COORDINATES
FROM DENVER.V_OSM_DEN_WAY
WHERE ID = 705859570) AS B;

// Give me all amenities from education category in a radius of 2,000 metres from a point
SELECT
*
FROM DENVER.V_OSM_DEN_AMENITY_EDUCATION
WHERE ST_DWITHIN(ST_POINT(-1.049212522000000e+02,
    3.969829250000000e+01),COORDINATES,2000);

// Give me all food and beverage Shops in a radius of 2,000 metres from a point

SELECT
*
FROM DENVER.V_OSM_DEN_SHOP_FOOD_BEVERAGES  
WHERE ST_DWITHIN(ST_POINT(-1.049632800000000e+02,
    3.974338330000000e+01),COORDINATES,2000);



//////////////////////


-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lng='-104.97300245114094';
set mc_lat='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lng='-105.00840763333615'; 
set loc_lat='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lng,$mc_lat) as melanies_cafe_point;
select st_makepoint($loc_lng,$loc_lat) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(
        st_makepoint($mc_lng,$mc_lat)
        ,st_makepoint($loc_lng,$loc_lat)
        ) as mc_to_cp;

