from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, to_date, year, month,
    row_number, round as spark_round,
    sum as spark_sum, avg, count
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window

# ------------------------------------------------
# 1. Create Spark session
# ------------------------------------------------
spark = (
    SparkSession.builder
    .appName("SparkCleaning")
    .getOrCreate()
)

# ------------------------------------------------
# 2. Read the CSV exported from Spark.xlsx
# ------------------------------------------------
# Adjust this path if needed
input_path = r"\Mac\Home\Downloads\orders.csv"

df_raw = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(input_path)
)

print("Raw rows:", df_raw.count())
df_raw.show(5, truncate=False)

# ------------------------------------------------
# 3. Standardize column names (lowercase, no spaces)
# ------------------------------------------------
df = df_raw.toDF(*[
    c.strip().lower().replace(" ", "_") for c in df_raw.columns
])

# Expecting: order_id, order_date, order_customer_id, order_status, sum

# ------------------------------------------------
# 4. Cast to correct data types
#    IMPORTANT: your order_date is in dd-MM-yyyy (e.g. 01-04-2024)
# ------------------------------------------------
df = (
    df.withColumn("order_id", col("order_id").cast(IntegerType()))
      .withColumn("order_customer_id", col("order_customer_id").cast(IntegerType()))
      # Parse dd-MM-yyyy correctly
      .withColumn("order_date", to_date(col("order_date"), "dd-MM-yyyy"))
      .withColumn("sum", col("sum").cast(DoubleType()))
)

# ------------------------------------------------
# 5. Clean text columns
# ------------------------------------------------
df = df.withColumn("order_status", upper(trim(col("order_status"))))

# ------------------------------------------------
# 6. Remove bad records / nulls
# ------------------------------------------------
valid_statuses = [
    "CLOSED",
    "COMPLETE",
    "PENDING",
    "PENDING_PAYMENT",
    "CANCELED",
    "PROCESSING"
]

df = (
    df.na.drop(subset=["order_id", "order_date"])   # drop rows without key info
      .filter(col("sum").isNotNull())
      .filter(col("sum") >= 0)                      # no negative amounts
      .filter(col("order_status").isin(valid_statuses))
)

# ------------------------------------------------
# 7. Remove duplicate order_id (keep latest by date)
# ------------------------------------------------
w = Window.partitionBy("order_id").orderBy(col("order_date").desc())

df = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .drop("rn")
)

# ------------------------------------------------
# 8. Add order_year and order_month columns
# ------------------------------------------------
df = (
    df.withColumn("order_year", year(col("order_date")))
      .withColumn("order_month", month(col("order_date")))
)

print("After cleaning:", df.count())
df.select(
    "order_id",
    "order_date",
    "order_year",
    "order_month",
    "order_customer_id",
    "order_status",
    "sum"
).show(20, truncate=False)

# ------------------------------------------------
# 9. Monthly Revenue Summary
# ------------------------------------------------
monthly_summary = (
    df.groupBy("order_year", "order_month")
      .agg(
          spark_sum("sum").alias("total_revenue"),
          avg("sum").alias("avg_order_value"),
          count("*").alias("total_orders")
      )
      .orderBy("order_year", "order_month")
)

print("Monthly summary:")
monthly_summary.show(truncate=False)

# ------------------------------------------------
# 10. Identify Top Revenue Month
# ------------------------------------------------
top_month = (
    monthly_summary.orderBy(col("total_revenue").desc())
                   .limit(1)
)

print("Top revenue month:")
top_month.show(truncate=False)

# ------------------------------------------------
# 11. Save monthly revenue summary to CSV (optional)
#     Uses pandas if available; otherwise skips export gracefully.
# ------------------------------------------------
output_path_monthly = r"C:\Mac\Home\Desktop\Spark_monthly_revenue.csv"

try:
    import pandas as pd  # will fail if pandas is not installed

    pdf = monthly_summary.toPandas()
    pdf.to_csv(output_path_monthly, index=False)
    print(f"Monthly revenue exported to {output_path_monthly}")

except ImportError as e:
    print("\n[INFO] Could not export to CSV because pandas is not installed.")
    print("       To enable CSV export, run:")
    print("       pip install pandas\n")
    print("Script finished successfully otherwise (Spark processing + summaries shown above).")

# ------------------------------------------------
# 12. Stop Spark session
# ------------------------------------------------
spark.stop()
