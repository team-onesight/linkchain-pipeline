import logging
import os

import boto3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook as AWS_S3Hook


class S3Hook(AWS_S3Hook):
    logger = logging.getLogger(__name__)
    """
    S3Hook은 AWS S3에 연결하기 위한 커스텀 훅입니다.

    :param aws_conn_id: airflow connection의 id. 없다면, 환경변수를 사용합니다.
    :type aws_conn_id: str
    """

    def __init__(self, *args, aws_conn_id: str = "aws_default", bucket_name, **kwargs) -> None:
        super().__init__(aws_conn_id, *args, **kwargs)
        self.bucket_name = bucket_name
        self.client = None

    def get_conn(self) -> boto3.client:
        """
        get connection
        """
        if self.client:
            return self.client

        if self.aws_conn_id:
            connection = self.get_connection(self.aws_conn_id)
            self.client = boto3.client(
                "s3",
                aws_access_key_id=connection.login,
                aws_secret_access_key=connection.password,
                region_name=connection.extra_dejson.get("region_name"),
            )
        else:
            self.client = boto3.client(
                "s3",
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                region_name=os.environ.get("AWS_DEFAULT_REGION"),
            )
        return self.client

    def upload_bytes(
        self,
        bytes_data: bytes,
        key: str,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: str | None = None,
    ) -> None:
        super().load_bytes(
            bytes_data=bytes_data,
            key=key,
            bucket_name=self.bucket_name,
            replace=replace,
            encrypt=encrypt,
            acl_policy=acl_policy,
        )

    def upload_file(self, file_path: str, key: str, extra_args: dict) -> bool:
        client = self.get_conn()
        try:
            client.upload_file(
                Filename=file_path,
                Bucket=self.bucket_name,
                Key=key,
                ExtraArgs=extra_args,
            )
            return True
        except Exception as e:
            logging.error("Upload failed:", exc_info=e)
            return False
