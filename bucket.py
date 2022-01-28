import boto3

from botocore.exceptions import ClientError
from pathlib import Path

class Bucket(object):
    """
    AWS S3 Bucket 操作する model
    """

    def __init__(self, bucket_name: str):
        """ initialization parameter

        params
        ------
            client(str): s3
            bucket_name(str): choice bucket name
            region(str): Region code
        """
        self.client = boto3.client('s3')
        self.resource_bucket = boto3.resource('s3')
        self.bucket_name = bucket_name
        self.region = 'ap-northeast-1'

    def create_bucket(self):
        """ Bucket を作成する処理

        params
        -------
            max_key(int): default: 2
                is_bucket_check fuc args
        """
        try:
            self.client.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={'LocationConstraint': self.region}
            )

        # 作成 Bucket名が既に存在するか判定。即処理停止
        except self.client.exceptions.BucketAlreadyExists as err:
            print('AlreadyExists')
            raise err

    def upload_data(self, upload_data: str):
        """
        S3に 1 データ upload する処理

        params
        ------
            upload_data: ex) sample.png
        """
        key = upload_data.split('/')[-1]

        with open(upload_data, 'rb') as upload_data_file:
            self.resource_bucket.Bucket(self.bucket_name).put_object(Key=key, Body=upload_data_file)


s3test = Bucket("study-test-9")
s3test.create_bucket()
