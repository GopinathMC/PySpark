--this below code will perform cumulative sum of unit price based on stockcode, if stockcode reappera those sum will get added up
select invoiceno,stockcode,customerid,unitprice,sum(unitprice) 
over(partition by stockcode order by invoiceno 
rows between unbounded preceding and current row)cumulative_sum
from retail_price;

--this is for cumulative average
select invoiceno,stockcode,customerid,unitprice,avg(unitprice) 
over(partition by stockcode order by invoiceno 
rows between unbounded preceding and current row)cumulative_avg
from retail_price;

--to get the minimum and maximum amount spent for particular invoice number and its details
with detail as (
select * from(
select invoiceno,stockcode,customerid,unitprice,
min(unitprice) over(partition by invoiceno order by invoiceno)min_price,
max(unitprice) over(partition by invoiceno order by invoiceno)max_price,
row_number() over (partition by invoiceno order by invoiceno) as r1
from retail_price)a where a.r1=1 )

select invoiceno,stockcode,customerid,unitprice,'minimum' as less_high
from retail_price
where concat(cast(invoiceno as string),cast(unitprice as string)) in 
(select concat(cast(invoiceno as string),cast(min_price as string)) from detail)

union all

select invoiceno,stockcode,customerid,unitprice,'maximum' as less_high
from retail_price
where concat(cast(invoiceno as string),cast(unitprice as string)) in 
(select concat(cast(invoiceno as string),cast(max_price as string)) from detail)

--finding values of before/after row values i.e.lead(col,offset,defaultval)
select invoiceno,stockcode,customerid,unitprice,
lag(unitprice,1,0) over(partition by invoiceno order by invoiceno) as lead_unit_price
from retail_price;
select invoiceno,stockcode,customerid,unitprice,
lead(unitprice,2,0) over(partition by invoiceno order by invoiceno) as lead_unit_price
from retail_price;               

--time problem
select a.user_id,a.episode_name,a.start,a.`end`,
sum(a.val) over(partition by a.user_id,a.episode_name order by a.user_id rows between unbounded preceding and current row)
as cumulative_sum
from(

select b.user_id,b.episode_name,b.start,b.`end`,
case when b.start<lag(b.`end`,1,0) over(partition by b.user_id,b.episode_name order by b.user_id) then 
(b.`end`-lag(b.`end`,1) over(partition by b.user_id,b.episode_name order by b.user_id))
else (b.`end`-b.start) end as val
from (select user_id,episode_name,case when start=1 then 0 else start end as start,`end` from showtime)b
)a;
