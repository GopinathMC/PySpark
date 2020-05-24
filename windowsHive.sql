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



