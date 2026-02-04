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

# COMMAND ----------

from pyspark.sql.functions import col, sha2, when, current_timestamp, lit

dbutils.widgets.text("ds", "", "Execution date (YYYY-MM-DD)")
# ds = dbutils.widgets.get("ds")
ds = "2026-02-03"
if not ds:
    ds = datetime.now().strftime("%Y-%m-%d")

print(f"Processing silver for date: {ds}")

bronze_path = f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/delta/bronze/ecom_orders"  # ← your bronze path
bronze_df = spark.read.format("delta").load(bronze_path).where(col("dt") == ds)

print(f"Bronze rows: {bronze_df.count()}")
bronze_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cell 3
# COMMAND ----------

silver_df = (
    bronze_df.dropDuplicates(["order_id"])
    .withColumn("email_hash", sha2(col("email"), 256))
    .withColumn("phone_masked", lit("XXXXXXXXXX"))
    .drop("email", "phone")
    .withColumn("ingest_time", current_timestamp())
    .withColumn("dt", lit(ds))
)

print(f"Silver rows after cleaning: {silver_df.count()}")
silver_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cell 4
# COMMAND ----------

silver_path = f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/delta/silver/ecom_orders_clean"  # same bucket, new prefix

silver_df.write.format("delta").mode("overwrite").partitionBy("dt").option(
    "mergeSchema", "true"
).save(silver_path)

print(f"Silver Delta table written to: {silver_path}")

# Verify
spark.read.format("delta").load(silver_path).where(col("dt") == ds).show(
    5, truncate=False
)
