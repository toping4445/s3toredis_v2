from multiprocessing import Pool
import multiprocessing as mp
import time
import os
import logging
import boto3
import json
from datetime import datetime
import snappy
from rediscluster import RedisCluster
import awswrangler as wr


logging.basicConfig(level=logging.INFO)

ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
SESSION_TOKEN = os.environ.get("SESSION_TOKEN")
BUCKET = os.environ.get("BUCKET", "sagemaker-yelo-test")
TABLE_NAME = os.environ.get("TABLE_NAME", "rms_features_feast")
RUN_EXPIRATION_JOB = os.environ.get("RUN_EXPIRATION_JOB", 0)
TESTING = os.environ.get("TESTING", 1)
STD_DATE = os.environ.get("STD_DATE", "2022-07-02")
TESTING =0
MODEL_NAME = os.environ.get("MODEL_NAME", "rms")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1")
org_date_format = '%Y-%m-%d'
converted_date_format = '%Y%m%d'
REDIS_HOST = os.environ.get("REDIS_HOST","feast-datahub.0437il.clustercfg.apn2.cache.amazonaws.com")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
PROCESS_NUM = os.environ.get("PROCESS_NUM", 4)
rc = RedisCluster(startup_nodes=[{"host": REDIS_HOST,"port": REDIS_PORT}], decode_responses=False,skip_full_coverage_check=True)

def func(s3_key):
    c_proc = mp.current_process()
    print("Running on Process", c_proc.name, "PID", c_proc.pid)
    child_pipe = rc.pipeline()

    url = f"s3://{BUCKET}/{s3_key}"
    df = wr.s3.read_parquet(path = url, dataset=False)

    # 모델에서 필요한 feature들만 남기기
    df.drop(['label', 'event_timestamp'], axis=1)
    df_json = df.to_json(orient="records")
    json_list = json.loads(df_json)

    for idx2, features in enumerate(json_list):
        if TESTING and idx2 == 1000:
            break

        yyyymmdd = (datetime.strptime(STD_DATE, org_date_format).strftime(converted_date_format))
        pay_account_id = str(features["pay_account_id"])
        features.pop("pay_account_id", None)
        key = ":".join([MODEL_NAME,MODEL_VERSION,pay_account_id])
        compressed_feature_json = snappy.compress(json.dumps(features))
        child_pipe.zadd(key, {compressed_feature_json:yyyymmdd})

    result = child_pipe.execute()
    success_cnt = sum(result)
    print(f"{c_proc.name}-{c_proc.pid} success_cnt : {success_cnt}")
    return success_cnt

if __name__ == '__main__':

    session = boto3.Session(
        region_name='ap-northeast-2',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(BUCKET)
    prefix = "data/" + TABLE_NAME + "/yyyymmdd=" + STD_DATE
    total_success_cnt = 0

    start = time.time()
    object_list = []
    for obj in bucket.objects.filter(Prefix=prefix):
        object_list.append(obj.key)

    pool = Pool(PROCESS_NUM)
    start = time.time()
    result = pool.map(func, object_list)
    total_success_cnt = sum(result)
    elapsed_time = time.time() - start
    print(f"total elapsed time : {elapsed_time:.5f} sec")
    print(f"total_success_cnt = {total_success_cnt}")

    pool.close()
    pool.join()