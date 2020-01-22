from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext()
hiveContext = HiveContext(sc)

#full cust-prod table
custProdDF = hiveContext.table("qas_product_fibi_ibp.md_customerproduct")
custProdDF.registerTempTable("CUSTPRODDF")

#dup data from cust-prod table
custProdDupDF = hiveContext.sql("select cp.prdid,cp.custid,cp.soldtoid,cp.zcmodeid,cp.zcustprdstatus,cp.sales_rep,cp.source,cp.business_unit,cp.soldtoproduct_type from CUSTPRODDF cp inner join(select prdid,custid,count(*) from CUSTPRODDF group by prdid,custid having count(*)>1)a on cp.prdid = a.prdid and cp.custid = a.custid and cp.source = 'MANUAL' order by cp.prdid")
#custProdDupDF.registerTempTable("CUSTPRODDUPDF")


#full new-cust-prod table
newcustProdDF = hiveContext.table("qas_product_fibi_ibp.md_newcustomerproduct")
#newcustProdDF.registerTempTable("NEWCUSTPRODDF")

#final new-cust-prod table
#newcustProdOriDF = hiveContext.sql("select * from NEWCUSTPRODDF a left join CUSTPRODDUPDF b on a.prdid = b.prdid and a.custid = b.custid where b.prdid is null and b.custid is null")
df1 = newcustProdDF.alias("df1")
df2 = custProdDupDF.alias("df2")
left = df1.join(df2,[df1['prdid']==df2['prdid'],df1['custid']==df2['custid']],how='left').select(df1['prdid'].alias("a_prd"),df1['custid'].alias("a_cust"),df1['zcmodeid'],df1['zcustprdstatus'],df1['sales_rep'],df1['source'],df1['business_unit'],df2['prdid'].alias("b_prd"),df2['custid'].alias("b_cust"))
newcustProdOriDF = left.filter(left['b_prd'].isNull() & left['b_cust'].isNull())
df3 =  newcustProdOriDF.select(newcustProdOriDF['a_prd'].alias("prdid"),newcustProdOriDF['a_cust'].alias("custid"),newcustProdOriDF['zcmodeid'],newcustProdOriDF['zcustprdstatus'],newcustProdOriDF['sales_rep'],newcustProdOriDF['source'],newcustProdOriDF['business_unit'])

#write it in md_newcustomerproduct table
df3.write.mode("overwrite").format("parquet").saveAsTable("qas_product_fibi_ibp.md_newcustomerproduct")

newcustProdDF.write.mode("overwrite").format("parquet").saveAsTable("qas_product_fibi_ibp.md_newcustprodcopy")




