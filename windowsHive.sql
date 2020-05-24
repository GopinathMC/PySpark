--this below code will perform cumulative sum of unit price based on stockcode, if stockcode reappera those sum will get added up

select invoiceno,stockcode,customerid,unitprice,sum(unitprice) 
over(partition by stockcode order by invoiceno 
rows between unbounded preceding and current row)cumulative_sum
from retail_price;
