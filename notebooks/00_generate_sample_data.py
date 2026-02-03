# notebooks/00_generate_sample_data.py
from faker import Faker
import pandas as pd
import boto3
from datetime import datetime
import os

fake = Faker()
ds = datetime.now().strftime("%Y-%m-%d")

# Generate small data for testing
data = []
for _ in range(1000):  # start small
    data.append(
        {
            "order_id": str(fake.uuid4()),
            "order_timestamp": fake.date_time_this_year().isoformat(),
            "user_id": str(fake.uuid4()),
            "product_id": str(fake.uuid4()),
            "quantity": fake.random_int(1, 5),
            "unit_price": round(fake.pyfloat(2, 2, positive=True) * 100, 2),
            "status": fake.random_element(
                ["pending", "shipped", "delivered", "cancelled"]
            ),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "city": fake.city(),
            "country": fake.country_code(),
        }
    )

df = pd.DataFrame(data)

# Save locally first
local_filename = f"orders_{ds}.json"
df.to_json(local_filename, orient="records", lines=True)
print(f"Saved locally: {local_filename}")

# Upload to S3
bucket_name = "gowtham-ecom-raw"  # ← CHANGE TO YOUR BUCKET NAME
s3_key = f"orders/dt={ds}/{local_filename}"

s3 = boto3.client("s3")  # uses ~/.aws/credentials automatically

try:
    s3.upload_file(local_filename, bucket_name, s3_key)
    print(f"Success! Uploaded to s3://{bucket_name}/{s3_key}")
except Exception as e:
    print(f"Error: {e}")
    print("Common fixes:")
    print("1. Check bucket name is correct")
    print("2. Run 'aws configure' and verify credentials")
    print("3. Ensure IAM user has s3:PutObject permission on this bucket")
    print("4. Check region matches (e.g. ap-south-1)")

# Optional: clean up local file
os.remove(local_filename)
