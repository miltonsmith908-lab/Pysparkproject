from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, countDistinct

# ===============================
# 1️⃣  Create Spark Session
# ===============================
spark = SparkSession.builder \
    .appName("ETL_PySpark_Example") \
    .getOrCreate()

# ===============================
# 2️⃣  Extract Phase
# ===============================
# Read data from CSV (you can also read from S3, JDBC, JSON, etc.)
input_path = "C:/Users/Hp/Desktop/VS/input_data.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

print("✅ Data Extracted:")
df.show(5)

# ===============================
# 3️⃣  Transform Phase
# ===============================
# Example transformations:
# - Rename columns
# - Filter rows
# - Handle missing values
# - Derive new columns
# - Aggregations

# Rename columns for consistency
df = df.withColumnRenamed("Customer ID", "customer_id") \
       .withColumnRenamed("Purchase Amount", "purchase_amount")

# Filter invalid or null entries
df_clean = df.filter(col("purchase_amount").isNotNull() & (col("purchase_amount") > 0))

# Add a new column for purchase category
df_transformed = df_clean.withColumn(
    "purchase_category",
    when(col("purchase_amount") > 500, "High Value")
    .when(col("purchase_amount") > 100, "Medium Value")
    .otherwise("Low Value")
)

# Example aggregation: average spend per customer
df_summary = df_transformed.groupBy("customer_id") \
    .agg(
        avg("purchase_amount").alias("avg_purchase"),
        countDistinct("purchase_category").alias("unique_categories")
    )

print("✅ Transformed Data:")
df_summary.show(5)

# ===============================
# 4️⃣  Load Phase
# ===============================
# Write output to Parquet (could also be S3, Delta, or a Database)
output_path = "C:/Users/Hp/Desktop/VS/output_data"
df_summary.write.mode("overwrite").parquet(output_path)

print("✅ Data Loaded successfully at:", output_path)

# ===============================
# 5️⃣  Stop Spark
# ===============================
spark.stop()
spark.stop()

