from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum as _sum, rank
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("Country-wise 3rd Highest Selling Product").getOrCreate()

# Load CSV files into DataFrames
transactions = spark.read.csv("transactions.csv", header=True, inferSchema=True)
customers = spark.read.csv("customer.csv", header=True, inferSchema=True)
products = spark.read.csv("product.csv", header=True, inferSchema=True)

# Filter transactions for the year 2023
transactions_2023 = transactions.filter(year("datetime") == 2023)

# Join DataFrames to get customer and product details
transactions_with_details = transactions_2023 \
    .join(customers, "customer_id") \
    .join(products, "product_id")

# Calculate total sales (price * quantity) for each product by country
sales_by_country_product = transactions_with_details \
    .groupBy("country", "product_id", "product_name") \
    .agg(_sum(col("price") * col("quantity")).alias("total_sales"))

# Define a window to rank products by sales within each country
window_spec = Window.partitionBy("country").orderBy(col("total_sales").desc())

# Add a rank column
ranked_sales = sales_by_country_product \
    .withColumn("rank", rank().over(window_spec))

# Filter for the 3rd highest selling product in each country
third_highest_sales = ranked_sales.filter(col("rank") == 3)

# Show the result
third_highest_sales.select("country", "product_name", "total_sales").show()


'''
dbutils.fs.mount()

Pass there
client id, adls location
client secret 
mount_name
refresh url.


'''

