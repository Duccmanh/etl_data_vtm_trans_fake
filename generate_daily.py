import os
import random
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# ======================================================
# CONFIG
# ======================================================

DAILY_VOLUME = 100000
NUM_USERS = 20000
CLUSTER_USERS = 1500

S3_BUCKET = "vtm-data-fake"
S3_PREFIX = "stogare/transaction/daily"
AWS_REGION = "ap-southeast-2"

AUTO_PAY_PRODUCTS = ["BILL_PAY", "TOPUP", "INSURANCE", "LOAN", "SAVING"]

product_services = {
    "TRANSFER": ["TRF_PHONE", "TRF_BANK", "TRF_CASH"],
    "BILL_PAY": ["BILL_ELECTRIC", "BILL_WATER", "BILL_INTERNET"],
    "TOPUP": ["TOPUP_PHONE", "TOPUP_DATA", "TOPUP_GAME"],
    "TRAVEL": ["BOOK_FLIGHT", "BOOK_TRAIN", "BOOK_HOTEL"],
    "SAVING": ["SAVE_ONLINE", "SAVE_AUTO", "SAVE_PROMO"],
    "LOAN": ["LOAN_SHORT", "LOAN_LONG", "LOAN_INSTANT"],
    "INSURANCE": ["INSUR_HEALTH", "INSUR_VEHICLE", "INSUR_PERSONAL"],
    "QR_PAY": ["QR_STORE", "QR_MARKET", "QR_CAFE"]
}

# ======================================================
# DATE LOGIC
# ======================================================

now_utc = datetime.utcnow()
now_vn = now_utc + timedelta(hours=7)
target_date = now_vn.date() - timedelta(days=1)

start_time = datetime.combine(target_date, datetime.min.time())
print(f"Generating data for date: {target_date}")

# ======================================================
# USER SETUP
# ======================================================

cluster_user_ids = set(random.sample(range(NUM_USERS), CLUSTER_USERS))
users = [f"849{random.randint(10000000, 99999999)}" for _ in range(NUM_USERS)]
auto_pay_tracker = defaultdict(set)

def random_time_within_day():
    seconds = random.randint(0, 86399)
    return start_time + timedelta(seconds=seconds)

records = []

# ======================================================
# DATA GENERATION
# ======================================================

while len(records) < DAILY_VOLUME:

    user_index = random.randint(0, NUM_USERS - 1)
    msisdn = users[user_index]

    product_code = random.choice(list(product_services.keys()))
    service_code = random.choice(product_services[product_code])

    request_date_dt = random_time_within_day()
    request_date = request_date_dt.strftime("%Y-%m-%d %H:%M:%S")  # STRING 100%

    month_key = f"{request_date_dt.year}-{request_date_dt.month}"

    process_code = "300001"
    if product_code in AUTO_PAY_PRODUCTS:
        if month_key not in auto_pay_tracker[msisdn] and random.random() < 0.3:
            process_code = "750001"
            auto_pay_tracker[msisdn].add(month_key)

    trans_amount = random.randint(10000, 600000)
    trans_fee = int(trans_amount * random.uniform(0.01, 0.05))

    first_error = random.choices(["00", "01", "02", "05"], weights=[92,4,2,2])[0]

    base_id = request_date_dt.strftime("%Y%m%d") + f"{random.randint(0,999999):06d}"

    records.append([
        base_id, msisdn, service_code, product_code,
        process_code, trans_amount, trans_fee,
        first_error, request_date,
        int(request_date_dt.strftime("%Y%m%d")),
        0, False
    ])

# ======================================================
# DATAFRAME
# ======================================================

columns = [
    "request_id","msisdn","service_code","product_code",
    "process_code","trans_amount","trans_fee",
    "error_code","request_date","partition_date",
    "retry_sequence","is_retry"
]

df = pd.DataFrame(records, columns=columns)

print(df.dtypes)  # DEBUG

# ======================================================
# WRITE PARQUET WITH FIXED SCHEMA (NO TIMESTAMP)
# ======================================================

schema = pa.schema([
    ("request_id", pa.string()),
    ("msisdn", pa.string()),
    ("service_code", pa.string()),
    ("product_code", pa.string()),
    ("process_code", pa.string()),
    ("trans_amount", pa.int64()),
    ("trans_fee", pa.int64()),
    ("error_code", pa.string()),
    ("request_date", pa.string()),   # 🔥 FORCE STRING
    ("partition_date", pa.int64()),
    ("retry_sequence", pa.int64()),
    ("is_retry", pa.bool_())
])

table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

file_name = f"transactions_{target_date}.parquet"
local_path = f"/tmp/{file_name}"

pq.write_table(table, local_path)

print("Parquet file created:", local_path)

# ======================================================
# UPLOAD TO S3
# ======================================================

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=AWS_REGION
)

partition_value = target_date.strftime("%Y%m%d")
s3_key = f"{S3_PREFIX}/partition_date={partition_value}/{file_name}"

s3.upload_file(local_path, S3_BUCKET, s3_key)

print("Upload successful:", s3_key)
