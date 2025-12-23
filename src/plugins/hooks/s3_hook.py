import boto3
import os

from airflow.sdk import BaseHook


class S3Hook(BaseHook):
    """
    S3Hook은 AWS S3에 연결하기 위한 커스텀 훅입니다.

    :param aws_conn_id: airflow connection의 id. 없다면, 환경변수를 사용합니다.
    :type aws_conn_id: str
    """

    def __init__(self, aws_conn_id: str = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
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
