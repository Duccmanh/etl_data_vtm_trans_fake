import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import boto3
import io

# =============================
# CONFIG
# =============================

NEW_USER_RATE = 0.01
INACTIVE_RATE = 0.003

TRANS_MIN = 50000
TRANS_MAX = 65000

S3_BUCKET = "vtm-data-fake"
S3_PREFIX_USER = "storage/user"
S3_PREFIX_TRANS = "storage/transaction"

AWS_REGION = "ap-southeast-2"

AUTO_PAY_PRODUCTS = ["BILL_PAY","TOPUP","INSURANCE","LOAN","SAVING"]

product_services = {
    "TRANSFER": ["TRF_PHONE","TRF_BANK","TRF_CASH"],
    "BILL_PAY": ["BILL_ELECTRIC","BILL_WATER","BILL_INTERNET"],
    "TOPUP": ["TOPUP_PHONE","TOPUP_DATA","TOPUP_GAME"],
    "TRAVEL": ["BOOK_FLIGHT","BOOK_TRAIN","BOOK_HOTEL"],
    "SAVING": ["SAVE_ONLINE","SAVE_AUTO","SAVE_PROMO"],
    "LOAN": ["LOAN_SHORT","LOAN_LONG","LOAN_INSTANT"],
    "INSURANCE": ["INSUR_HEALTH","INSUR_VEHICLE","INSUR_PERSONAL"],
    "QR_PAY": ["QR_STORE","QR_MARKET","QR_CAFE"]
}

products = list(product_services.keys())

s3 = boto3.client("s3",region_name=AWS_REGION)

# =============================
# FIND LATEST PARTITION ON S3
# =============================

def get_latest_partition():

    paginator = s3.get_paginator("list_objects_v2")

    partitions = []

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX_USER
    ):

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if "partition_date=" in key:

                part = key.split("partition_date=")[1].split("/")[0]
                partitions.append(part)

    if not partitions:
        return None

    return max(partitions)

# =============================
# LOAD PREVIOUS SNAPSHOT
# =============================

latest_partition = get_latest_partition()

if latest_partition is None:
    raise Exception("No snapshot found on S3")

print("Latest partition:", latest_partition)

def load_user_snapshot(partition_date):

    key = f"{S3_PREFIX_USER}/partition_date={partition_date}/user_{partition_date}.parquet"

    print("Loading snapshot:", key)

    obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key=key
    )

    buffer = io.BytesIO(obj["Body"].read())

    df = pd.read_parquet(buffer)

    return df
    
user_df = load_user_snapshot(latest_partition)

current_date = datetime.strptime(latest_partition, "%Y%m%d").date() + timedelta(days=1)

print("Loaded users:", len(user_df))

print("Active users:", (user_df.status=="active").sum())

# =============================
# LOOP DATE
# =============================

# chạy đúng 1 ngày
partition_date = current_date.strftime("%Y%m%d")

print("Processing", partition_date)

# =============================
# USER UPDATE
# =============================

active_users = user_df[user_df.status=="active"].msisdn.to_numpy()

new_user_count = int(len(active_users)*NEW_USER_RATE)

new_users = [f"849{random.randint(10000000,99999999)}" for _ in range(new_user_count)]

new_df = pd.DataFrame({
    "msisdn":new_users,
    "register_date":[current_date]*new_user_count,
    "status":["active"]*new_user_count,
    "inactive_date":[None]*new_user_count
})

user_df = pd.concat([user_df,new_df],ignore_index=True)

inactive_count = int(len(active_users)*INACTIVE_RATE)

if inactive_count > 0:

    inactive_sample = np.random.choice(active_users,inactive_count,replace=False)

    user_df.loc[user_df.msisdn.isin(inactive_sample),"status"]="inactive"
    user_df.loc[user_df.msisdn.isin(inactive_sample),"inactive_date"]=current_date

active_users = user_df[user_df.status=="active"].msisdn.to_numpy()

# =============================
# USER CLUSTER
# =============================

np.random.shuffle(active_users)

n=len(active_users)

power_users=active_users[:int(n*0.05)]
normal_users=active_users[int(n*0.05):int(n*0.25)]
low_users=active_users[int(n*0.25):]

# =============================
# TRANSACTION GENERATION
# =============================

trans_volume = random.randint(TRANS_MIN,TRANS_MAX)

users_sample = np.random.choice(
    np.concatenate([power_users, normal_users, low_users]),
    trans_volume
)

product_sample = np.random.choice(products,trans_volume)

service_sample = [
    random.choice(product_services[p]) for p in product_sample
]

amount = np.random.randint(10000,600000,trans_volume)

fee = (amount*np.random.uniform(0.01,0.05,trans_volume)).astype(int)

error_code = np.random.choice(
    ["00","01","02","05"],
    trans_volume,
    p=[0.92,0.04,0.02,0.02]
)

print(
    f"{partition_date} | users={len(user_df)} | active={(user_df.status=='active').sum()} | trans={trans_volume}"
)

# =============================
# TIME GENERATION
# =============================

start_day=datetime.combine(current_date,datetime.min.time())

seconds=np.random.randint(0,86400,trans_volume)

times=[start_day+timedelta(seconds=int(s)) for s in seconds]

request_date=[t.strftime("%Y-%m-%d %H:%M:%S") for t in times]

request_id=[t.strftime("%Y%m%d")+f"{random.randint(0,999999):06d}" for t in times]

process_code=["300001"]*trans_volume

trans_df=pd.DataFrame({
    "request_id":request_id,
    "msisdn":users_sample,
    "service_code":service_sample,
    "product_code":product_sample,
    "process_code":process_code,
    "trans_amount":amount,
    "trans_fee":fee,
    "error_code":error_code,
    "request_date":request_date,
    "partition_date":[partition_date]*trans_volume
})

# =============================
# UPLOAD USER
# =============================

user_buffer=io.BytesIO()
user_df.to_parquet(user_buffer,index=False)
user_buffer.seek(0)

user_key=f"{S3_PREFIX_USER}/partition_date={partition_date}/user_{partition_date}.parquet"

s3.put_object(
    Bucket=S3_BUCKET,
    Key=user_key,
    Body=user_buffer.getvalue()
)

# =============================
# UPLOAD TRANSACTION
# =============================

trans_buffer=io.BytesIO()
trans_df.to_parquet(trans_buffer,index=False)
trans_buffer.seek(0)

trans_key=f"{S3_PREFIX_TRANS}/partition_date={partition_date}/transaction_{partition_date}.parquet"

s3.put_object(
    Bucket=S3_BUCKET,
    Key=trans_key,
    Body=trans_buffer.getvalue()
)

print("DONE")



