import boto3
import json
import uuid
import time
import random
from faker import Faker

# --- CONFIGURATION ---
# Matches your Terraform locals and provider
STREAM_NAME = 'clickstream-project-stream'
REGION = 'ap-south-1' 

faker = Faker()
kinesis = boto3.client('kinesis', region_name=REGION)

def send_to_kinesis(payload, partition_key):
    """Helper to push data to the stream"""
    try:
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(payload),
            PartitionKey=str(partition_key)
        )
        return response
    except Exception as e:
        print(f"Error sending data: {e}")

# --- 1. GENERATE USERS (CDC Initial Load) ---
# We create these first so our Clickstream events can reference real user IDs
print("🚀 Generating 10 initial users...")
active_users = []
for _ in range(10):
    user_id = random.randint(1000, 9999)
    user_payload = {
        "metadata": {
            "source": "database",
            "table": "users",
            "op": "I",
            "ts": float(time.time()) # Ensure it's a Double for Glue
        },
        "data": {
            "user_id": user_id,
            "name": faker.name(),
            "email": faker.email(),
            "country": "India",
            "url": None,          # Union Schema: Click fields are NULL for Users
            "event_id": None,
            "platform": None,
            "duration_sec": None
        }
    }
    active_users.append(user_payload['data'])
    send_to_kinesis(user_payload, user_id)

# --- 2. GENERATE CDC UPDATES (SCD Type 1 Test) ---
print("🔄 Generating a user update...")
user_to_update = random.choice(active_users)
cdc_payload = {
    "metadata": {
        "source": "database",
        "table": "users",
        "op": "U", 
        "ts": float(time.time())
    },
    "data": {
        "user_id": user_to_update['user_id'],
        "name": user_to_update['name'],
        "email": "updated_" + faker.email(),
        "country": "India",
        "url": None,
        "event_id": None,
        "platform": None,
        "duration_sec": None
    }
}
send_to_kinesis(cdc_payload, user_to_update['user_id'])

# --- 3. GENERATE CLICKSTREAM (Sessionization Test) ---
# We'll generate 50 events to ensure we have enough data for "Gaps and Islands" logic
print("🖱️ Generating 50 clickstream events...")
for i in range(50):
    user = random.choice(active_users)
    click_payload = {
        "metadata": {
            "source": "frontend",
            "table": "clickstream",
            "op": "A",
            "ts": float(time.time())
        },
        "data": {
            "user_id": user['user_id'], # Linking to our users
            "event_id": str(uuid.uuid4()),
            "url": faker.uri_path(),
            "platform": random.choice(['mobile', 'desktop', 'tablet']),
            "duration_sec": random.randint(1, 300),
            "name": None,               # Union Schema: User fields are NULL for Clicks
            "email": None,
            "country": None
        }
    }
    send_to_kinesis(click_payload, user['user_id'])
    
    # Small sleep to simulate real-time traffic and spread out timestamps
    if i % 10 == 0:
        time.sleep(1)

print("\n✅ Data generation complete!")
print(f"Check your S3 bucket in about 60-90 seconds for the Parquet files.")