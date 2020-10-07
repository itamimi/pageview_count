-- create schema and tables in BI warehouse  snowflake

-- create production Schema
create schema bi_db ;

-- Create analytic schema where transformed and denormalised data is stored
create schema bi_analytic ;


-- create Tables

-- extraction tables in production

create table bi_db.users_extract
(
id bigint ,
postcode varchar(20)
);



create table bi_db.pageviews_extract
(
user_id bigint ,
url varchar(255) ,
pageview_datetime timestamp
);


-- create analytic tables
create  table  bi_analytic.Pageviews_user_history
(
Pageviews_user_history_id  bigint AUTOINCREMENT (1,1) ,
pageview_datetime timestamp ,
user_id bigint,
url varchar(255) ,
postcode varchar(20)
);


create  table  bi_analytic.pageviews_postcode_now_count
(
date date,   -- yyyy-mm-dd
hour int,
postcode varchar(20),
pageviews_count bigint

);


create  table  bi_analytic.pageviews_postcode_history_count
(
date date,   -- yyyy-mm-dd
hour int,
postcode varchar(20),
pageviews_count bigint

);
