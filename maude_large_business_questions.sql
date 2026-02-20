use maude_db;

select * from maude_mdr_raw limit 10;

SHOW TABLES;

select count(*) from maude_mdr_raw;

create table maude_mdr_staging as
select * from maude_mdr_raw  where 1 = 0;

insert into maude_mdr_staging
select * from maude_mdr_raw;
select count(*) from maude_mdr_staging1;

-- Total number of adverse events

show columns from maude_mdr_staging1;

select adverse_event_flag, count(*)
as total_adverse_events from maude_mdr_staging1
group by adverse_event_flag;

select distinct manufacturer_name from maude_t limit 200;
select adverse_event_flag, count(*)
as total_adverse_events from maude_t
group by adverse_event_flag;

-- Top 10 most problematic devices

select device_name, count(*) as defect_counts
from maude_t
group by device_name order by defect_counts desc;


-- Serious safety risk companies
-- Death events by manufacturer
select MANUFACTURER_G1_NAME, 
sum(case when event_type = 'IN' then 1 else 0 end) as In_counts,
sum(case when event_type = 'M' then 1 else 0 end) as M_counts,
sum(case when event_type = 'D' then 1 else 0 end) as D_counts,
sum(case when event_type = 'O' then 1 else 0 end) as O_counts
from maude_t
group by MANUFACTURER_G1_NAME
order by D_counts desc, In_counts desc, M_counts desc, O_counts desc;

-- Death and injury - major incidents are reported from Thoratec corporation followed by Danvers

-- Malfunction trend over time

select year(date_added) as yr,
sum(case when event_type = 'M' then 1 else 0 end) as M_counts
from maude_t
group by year(date_added)
order by yr asc;

describe maude_t;

update maude_t
set date_added = str_to_date(date_added, '%m/%d/%Y');

select year(date_added) as yr,
sum(case when event_type = 'M' then 1 else 0 end) as M_counts
from maude_t
group by year(date_added)
order by yr asc;

-- Events by country

select MANUFACTURER_COUNTRY_CODE, count(*) as defect_counts_country
from maude_t
group by MANUFACTURER_COUNTRY_CODE order by defect_counts_country desc;

-- Who reported the most incidents  -- manufacturers
select report_source_code, count(*) as defect_counts_country
from maude_t
group by report_source_code order by defect_counts_country desc;

--  Top 3 manufacturers per year

with cte1 as (select MANUFACTURER_G1_NAME, year(date_added) as yr, count(*) as report_counts
from maude_t group by MANUFACTURER_G1_NAME, yr order by report_counts desc),
cte2 as (select MANUFACTURER_G1_NAME, yr, report_counts, 
rank() over (partition by yr order by report_Counts desc) as rnk
from cte1)
select MANUFACTURER_G1_NAME, yr, report_counts from cte2
where rnk <= 3 and yr is not null and MANUFACTURER_G1_NAME != '';

-- Year-over-year growth in adverse events

update maude_t
set date_of_event = str_to_date(date_of_event, '%m/%d/%Y')
where date_of_event != '';

with cte1 as (select year(date_of_event) as yr, count(*) as cnt from maude_t
group by yr),
cte2 as (select yr, cnt, lag(cnt) over (order by yr asc) as prev_cnt from cte1)
select yr, cnt, prev_cnt, (cnt-prev_cnt)*100/prev_cnt as growth_per from cte2
where prev_cnt is not null;

-- Which manufacturers show increasing risk trend?
with cte1 as (select MANUFACTURER_G1_NAME, year(date_of_event) as yr, count(*)
as cnt from maude_t group by MANUFACTURER_G1_NAME, yr),
cte2 as (select MANUFACTURER_G1_NAME, yr, cnt, lag(cnt) over (partition by MANUFACTURER_G1_NAME
order by yr asc) as prev_cnt from cte1),
cte3 as (select MANUFACTURER_G1_NAME, yr, cnt, prev_cnt, cnt-prev_cnt as increase
from cte2 order by MANUFACTURER_G1_NAME asc, increase asc)
select MANUFACTURER_G1_NAME from cte3 group by 
MANUFACTURER_G1_NAME having min(increase) > 0;

-- Identify sudden spike in events

with cte1 as (select year(date_of_event) as yr, count(*) as cnt
from maude_t group by yr),
cte2 as (select yr,  cnt, lag(cnt) over (order by  yr) as prev_cnt,
lag(yr) over (order by yr) as prev_year
 from cte1)
select  yr, cnt, prev_year, prev_cnt from cte2
where (cnt-prev_cnt)/prev_cnt > 0.5;

