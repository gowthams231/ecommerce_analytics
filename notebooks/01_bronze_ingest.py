# Databricks notebook source
# DBTITLE 1,Configure S3 credentials (one-time setup)
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

# DBTITLE 1,Step 3: Read data from S3
# Construct S3 URI with embedded credentials
s3_uri = f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/orders/dt=2026-02-03/*.json"

df = spark.read.json(s3_uri)
df.count()

# COMMAND ----------

# DBTITLE 1,Write to Bronze Delta Table
from pyspark.sql.functions import lit

bronze_path = f"s3a://{aws_access_key}:{encoded_secret_key}@gowtham-ecom-raw/delta/bronze/ecom_orders"

ds = "2026-02-03"  # Define the date partition value
df = df.withColumn("dt", lit(ds))

df.write.format("delta").mode("append").partitionBy("dt").option(
    "mergeSchema", "true"
).save(bronze_path)

print(f"Bronze written to: {bronze_path}")

# Verify
spark.read.format("delta").load(bronze_path).where(f"dt = '{ds}'").show(5)

# COMMAND ----------

spark.read.format("delta").load(bronze_path).where(f"dt = '{ds}'").count()

# COMMAND ----------
