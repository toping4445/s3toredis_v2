#!/usr/bin/env python

import pyarrow.parquet as pq
import s3fs
from rediscluster import RedisCluster
import logging
import json
import sys
import time
import os
import boto3
from datetime import datetime,timedelta
import snappy

logging.basicConfig(level=logging.INFO)

ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
SESSION_TOKEN = os.environ.get("SESSION_TOKEN")
STD_DATE = os.environ.get("STD_DATE", "2022-07-01")
TTL = os.environ.get("TTL", 3)
MIN_KEY_VALUE = os.environ.get("MIN_KEY_VALUE", 1)
MAX_KEY_VALUE = os.environ.get("MAX_KEY_VALUE", 2500000)
BUCKET = os.environ.get("BUCKET", "sagemaker-yelo-test")
TABLE_NAME = os.environ.get("TABLE_NAME", "rms_features_feast")
REDIS_HOST = os.environ.get("REDIS_HOST","feast-datahub.0437il.clustercfg.apn2.cache.amazonaws.com")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
MODEL_NAME = os.environ.get("MODEL_NAME", "rms")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1")
RUN_EXPIRATION_JOB = os.environ.get("RUN_EXPIRATION_JOB", 0)
TESTING = os.environ.get("TESTING", 1)



s3_fs = s3fs.S3FileSystem(key=ACCESS_KEY, secret = SECRET_KEY, token = SESSION_TOKEN)
logging.info(f"REDIS_HOST : {REDIS_HOST}")
logging.info(f"TESTING : {str(TESTING)}")
logging.info(f"RUN_EXPIRATION_JOB : {str(RUN_EXPIRATION_JOB)}")


rc = RedisCluster(startup_nodes=[{"host": REDIS_HOST,"port": REDIS_PORT}], decode_responses=False,skip_full_coverage_check=True)
pipe = rc.pipeline()

session = boto3.Session(
    region_name = 'ap-northeast-2',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN
)
s3 = session.resource('s3')
bucket = s3.Bucket(BUCKET)
org_date_format = '%Y-%m-%d'
converted_date_format = '%Y%m%d'

#TTL 지난 데이터 삭제 로직 추가
if RUN_EXPIRATION_JOB:
    expiration_date = (datetime.strptime(STD_DATE, org_date_format) - timedelta(days=3)).strftime(converted_date_format)
    start = time.time()

    for current_key in range(MIN_KEY_VALUE,MAX_KEY_VALUE):
        pipe.zremrangebyscore("rms:v1:"+str(current_key),expiration_date,expiration_date)

    result = pipe.execute()
    end = time.time()

    print(f"expiration job elapsed time : {end - start:.5f} sec")
    print(f"deleted values cnt : {str(sum(result))}")

prefix = "data/"+TABLE_NAME+"/yyyymmdd="+STD_DATE
total_size = 0
total_success_cnt = 0
flag1 = time.time()

for idx, object in enumerate(bucket.objects.filter(Prefix=prefix)):
    if TESTING and idx ==1:
        break

    url = f"s3://{BUCKET}/{str(object.key)}"
    df = pq.ParquetDataset(
        url,
        filesystem=s3_fs).read_pandas().to_pandas()
    flag2 = time.time()
    if TESTING:
        print(f"itr{idx} - elapsed time to read parquet file as df : {flag2 - flag1:.5f} sec")

    # 모델에서 필요한 feature들만 남기기
    # df = df[[feature_list_for_certain_model_ver]]
    df.drop(['label', 'event_timestamp'], axis=1)
    df_json = df.to_json(orient="records")
    json_list = json.loads(df_json)

    for idx2, features in enumerate(json_list):
        if TESTING and idx2 == 1:
            break

        yyyymmdd = (datetime.strptime(STD_DATE, org_date_format).strftime(converted_date_format))
        pay_account_id = str(features["pay_account_id"])
        features.pop("pay_account_id", None)
        key = ":".join([MODEL_NAME,MODEL_VERSION,pay_account_id])
        compressed_feature_json = snappy.compress(json.dumps(features))
        pipe.zadd(key, {compressed_feature_json:yyyymmdd})

    flag3 = time.time()
    print(f"itr{idx} - elapsed time to convert all features to JSON : {flag3 - flag2:.5f} sec")
    result = pipe.execute()
    flag4 = time.time()
    print(f"itr{idx} - elapsed time for redis job to finish : {flag4 - flag3:.5f} sec")
    total_success_cnt += sum(result)

flag5 = time.time()
print(f"total elapsed time : {flag5 - flag1:.5f} sec")
print("total_success_cnt : "+str(total_success_cnt))

