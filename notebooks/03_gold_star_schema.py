# Databricks notebook source
# Retrieve your S3 credentials from secrets
aws_access_key = dbutils.secrets.get(scope="s3-credentials", key="aws-access-key-id")
aws_secret_key = dbutils.secrets.get(
    scope="s3-credentials", key="aws-secret-access-key"
)

# URL encode the secret key (in case it contains special characters like /)
import urllib.parse

encoded_secret_key = urllib.parse.quote(aws_secret_key, safe="")

print("✓ S3 credentials retrieved and encoded!")
print("Note: On serverless, credentials must be embedded in each S3 URI")

# COMMAND ----------

from pyspark.sql.functions import col

# dbutils.widgets.text("ds", "", "Execution date (YYYY-MM-DD)")

# ds = dbutils.widgets.get("ds") or datetime.now().strftime("%Y-%m-%d")

ds = "2026-02-03"
silver_path = f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/delta/silver/ecom_orders_clean"
silver_df = spark.read.format("delta").load(silver_path).where(col("dt") == ds)

print(f"Silver rows: {silver_df.count()}")
silver_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql.functions import (
    expr,
    year,
    month,
    dayofmonth,
    date_format,
    monotonically_increasing_id,
)

# dim_date (generate for a range – simple version for one day)
dim_date = (
    spark.range(0, 365 * 3)
    .selectExpr("date_add('2025-01-01', cast(id as int)) as full_date")
    .withColumn("date_key", expr("cast(date_format(full_date, 'yyyyMMdd') as int)"))
    .withColumn("year", year("full_date"))
    .withColumn("month", month("full_date"))
    .withColumn("day", dayofmonth("full_date"))
    .withColumn("day_of_week", date_format("full_date", "EEEE"))
)

dim_date_path = (
    f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/delta/gold/dim_date"
)
dim_date.write.format("delta").mode("overwrite").save(dim_date_path)

# dim_users (distinct from silver – no PII)
dim_users = (
    silver_df.select("user_id", "city", "country")
    .distinct()
    .withColumn("user_key", monotonically_increasing_id())
)

dim_users_path = (
    f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/delta/gold/dim_users"
)
dim_users.write.format("delta").mode("overwrite").save(dim_users_path)

# dim_products (distinct)
dim_products = (
    silver_df.select("product_id", "unit_price")
    .distinct()
    .withColumn("product_key", monotonically_increasing_id())
)

dim_products_path = f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/delta/gold/dim_products"
dim_products.write.format("delta").mode("overwrite").save(dim_products_path)

# dim_status (small static table)
dim_status_data = [("pending", 1), ("shipped", 2), ("delivered", 3), ("cancelled", 4)]
dim_status = spark.createDataFrame(dim_status_data, ["status_name", "status_key"])

dim_status_path = f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/delta/gold/dim_status"
dim_status.write.format("delta").mode("overwrite").save(dim_status_path)

# COMMAND ----------

# DBTITLE 1,Cell 4
from pyspark.sql.functions import lit

fact_df = (
    silver_df.alias("silver")
    .join(
        dim_date.alias("date"),
        col("silver.order_timestamp").cast("date") == col("date.full_date"),
        "left",
    )
    .join(
        dim_users.alias("users"), col("silver.user_id") == col("users.user_id"), "left"
    )
    .join(
        dim_products.alias("products"),
        col("silver.product_id") == col("products.product_id"),
        "left",
    )
    .join(
        dim_status.alias("status"),
        col("silver.status") == col("status.status_name"),
        "left",
    )
    .select(
        col("date.date_key"),
        col("users.user_key"),
        col("products.product_key"),
        col("status.status_key"),
        col("silver.order_id"),
        col("silver.quantity"),
        (col("silver.quantity") * col("products.unit_price")).alias("revenue"),
        lit(1).alias("order_count"),
    )
)

fact_path = f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/delta/gold/fact_order_lines"
fact_df.write.format("delta").mode("overwrite").partitionBy("date_key").option(
    "mergeSchema", "true"
).save(fact_path)

print(f"Fact table written to: {fact_path}")
spark.read.format("delta").load(fact_path).where(col("date_key") == 20260203).show(5)
