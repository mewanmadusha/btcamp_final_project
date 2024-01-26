-- use role accountadmin;

-- create role junior_dba;
-- grant role junior_dba to user MEWANMADHUSHA;SNOWFLAKE_SAMPLE_DATA

-- use role junior_dba;


-- use role accountadmin;
-- grant usage on database citibike to role junior_dba;
-- grant usage on database weather to role junior_dba;

-- use role accountadmin;
-- use warehouse compute_wh;
-- use database weather;
-- use schema public;


-- drop share if exists trips_share;
-- drop database if exists citibike;
-- drop database if exists weather;
-- drop warehouse if exists analytics_wh;
-- drop role if exists junior_dba;

-- USE DATABASE SNOWFLAKE; 
SELECT *
FROM ACCOUNT_USAGE.QUERY_HISTORY
WHERE USER_NAME = 'MEWANMADHUSHA' AND EXECUTION_STATUS = 'SUCCESS' AND QUERY_TEXT LIKE '%SELECT%' OR QUERY_TEXT LIKE '%CREATE%';
