print ("hello world")
if 5>2:
    print("Five is greater than two!")

print("Hello"); print("How are you?"); print("Bye bye!")
print(2+3)

a,b,c=19,5,9
if a<c:
    print(b)
else:
    print(c)


# Declare a list containing duplicate numbers, e.g., [1, 2, 2, 3, 3, 3, 4], and store it in a variable

# Write a function that accepts a list as an argument and returns a new list with duplicates removed while preserving the original order

# Print the output of the execution of the function to the console
# Expected output:
# [1, 2, 3, 4]

list = [1, 2, 2, 3, 3, 3, 4]

list2 = []

def func(value): 
    for i in range(len(value)):
        if value[i-1] != value[i]:
            list2.append(value[i])

    return list2

print(func(list))

import sys 
print(sys.version)
exit()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, to_date, year, month,
    row_number, round as spark_round
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("SparkCleaning")
    .getOrCreate()
)
# Read the CSV exported from Spark.xlsx
input_path = r"C:\Mac\Home\Desktop\Spark.csv"  

df_raw = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(input_path)
)

# 1. Standardize column names (lowercase, no spaces)
df = df_raw.toDF(*[
    c.strip().lower().replace(" ", "_") for c in df_raw.columns
])

# Now columns should look like:
# order_id, order_date, order_customer_id, order_status, sum

# 2. Cast to correct data types

df = (
    df.withColumn("order_id", col("order_id").cast(IntegerType()))
      .withColumn("order_customer_id", col("order_customer_id").cast(IntegerType()))
      # adjust date format if needed (e.g. "dd-MM-yyyy")
      .withColumn("order_date", to_date(col("order_date")))
      .withColumn("sum", col("sum").cast(DoubleType()))
)

# 3. Clean text columns

df = df.withColumn("order_status", upper(trim(col("order_status"))))

# 4. Remove bad records / nulls
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

# 5. Remove duplicate order_id (keep latest by date)

w = Window.partitionBy("order_id").orderBy(col("order_date").desc())

df = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .drop("rn")
)

# ------------------------------------------------
# 6. Monthly Revenue Summary
# ------------------------------------------------
monthly_summary = (
    df_clean.groupBy("order_year", "order_month")
            .agg(
                spark_sum("sum").alias("total_revenue"),
                avg("sum").alias("avg_order_value"),
                count("*").alias("total_orders")
            )
            .orderBy("order_year", "order_month")
)

monthly_summary.show(truncate=False)

# ------------------------------------------------
# 7. Identify Top Revenue Month
# ------------------------------------------------
top_month = (
    monthly_summary.orderBy(col("total_revenue").desc())
                   .limit(1)
)

top_month.show()

# ------------------------------------------------
# 8. Optional: Save monthly revenue summary to CSV
# ------------------------------------------------
output_path_monthly = r"C:\Mac\Home\Desktop\Spark_monthly_revenue"

(
    monthly_summary.coalesce(1)
                   .write
                   .mode("overwrite")
                   .option("header", True)
                   .csv(output_path_monthly)
)

