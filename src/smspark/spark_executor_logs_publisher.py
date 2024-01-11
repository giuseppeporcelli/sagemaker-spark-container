# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""Thread to copy Spark executor logs to S3."""
import logging
import os
import os.path
import time
import boto3
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
)
log = logging.getLogger("sagemaker-spark-executor-logs")

class SparkExecutorLogsPublisher(Thread):
    """Child thread to copy the Spark executor logs to Amazon S3 on an interval.
    """

    def __init__(
        self,
        spark_executor_logs_s3_bucket: str,
        spark_executor_logs_s3_prefix="",
        local_spark_executor_logs_dir="/var/log/yarn",
        copy_interval: int = 20
    ) -> None:
        """Initialize."""
        Thread.__init__(self)
        self._stop_publishing = False
        self._copy_interval = copy_interval
        self.spark_executor_logs_s3_bucket = spark_executor_logs_s3_bucket
        self.spark_executor_logs_s3_prefix = spark_executor_logs_s3_prefix
        self.local_spark_executor_logs_dir = local_spark_executor_logs_dir
        self._s3_client = boto3.client("s3")

    def run(self) -> None:
        """Publishes Spark executor logs to the given S3 URL.
        """
        log.info("Start copying Spark executor logs to S3.")

        while not self._stop_publishing:
            self._upload_spark_executor_logs()
            time.sleep(self._copy_interval)

        self._upload_spark_executor_logs()

        log.info("Finished copying Spark executor logs to S3.")

    def down(self) -> None:
        """Stops publishing Spark executor logs."""
        self._stop_publishing = True

    def _upload_spark_executor_logs(self) -> None:
        if os.path.exists(self.local_spark_executor_logs_dir):
            for path, dirs, files in os.walk(self.local_spark_executor_logs_dir):
                for file in files:
                    s3_file_prefix = os.path.normpath(self.spark_executor_logs_s3_prefix + "/" + path + '/' + file)
                    file_local = os.path.join(path, file)
                    self._s3_client.upload_file(file_local,
                                                self.spark_executor_logs_s3_bucket,
                                                s3_file_prefix)
