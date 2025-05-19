from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("SalesProcessing") \
    .config("spark.jars", "/Users/narendrabayutamaw/Documents/Apache Hop - Neo4J/postgresql-42.7.5.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

jdbc_url = "jdbc:postgresql://ep-bitter-mountain-a1nthy4b-pooler.ap-southeast-1.aws.neon.tech/apachehop?sslmode=require"

properties = {
    "user": "apachehop_owner",
    "password": "npg_NRcK7ZQm1gCn",
    "driver": "org.postgresql.Driver"
}


sales = spark.read.jdbc(jdbc_url, "stg.sales", properties=properties)
products = spark.read.jdbc(jdbc_url, "stg.products", properties=properties)
dim_sales = spark.read.jdbc(jdbc_url, "dm.dim_sales", properties=properties)


sales = sales.withColumn("discount", F.coalesce("discount", F.lit(0)))
sales = sales.withColumn("discmultiplier", (100 - F.col("discount")) / 100)


joined = sales.join(products, "productid", "left")
joined = joined.withColumn("totalsales", F.col("price") * F.col("quantity") * F.col("discmultiplier"))


agg_totalsales = joined.groupBy("productid").agg(F.sum("totalsales").alias("totalsales"))


avg = joined.groupBy("productid").agg(
    (F.sum("totalsales") / F.when(F.sum("quantity") != 0, F.sum("quantity")).otherwise(1)).alias("avgprice")
)

# Rename the columns from dim_sales to avoid ambiguity
dim_sales_renamed = dim_sales.withColumnRenamed("totalsales", "dim_totalsales") \
                             .withColumnRenamed("avgprice", "dim_avgprice")


insert_ts = dim_sales_renamed.join(
    agg_totalsales, on="productid", how="left"
).withColumn(
    "totalsales",
    F.coalesce(agg_totalsales["totalsales"], F.col("dim_totalsales"))
)


insert_avg = insert_ts.join(
    avg, on="productid", how="left"
).withColumn(
    "avgprice",
    F.coalesce(avg["avgprice"], insert_ts["dim_avgprice"])
)


final_df = insert_avg.select(
	"productid",
	"productname",
	"categoryid",
	"class",
	"price",
	"avgdiscount",
	"quantity",
	"totalsales",
	"avgprice"
)

final_df.write.jdbc(jdbc_url, "dm.dim_sales", mode="overwrite", properties=properties)

spark.stop()