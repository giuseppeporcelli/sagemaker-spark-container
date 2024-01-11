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
"""Thread to copy Spark driver logs to S3."""
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
log = logging.getLogger("sagemaker-spark-driver-logs")

class SparkDriverLogsPublisher(Thread):
    """Child thread to copy the Spark driver logs to Amazon S3 on an interval.
    """

    def __init__(
        self,
        spark_driver_logs_s3_bucket: str,
        spark_driver_logs_s3_prefix="",
        local_spark_driver_logs_file="/var/log/spark_driver_logs.txt",
        copy_interval: int = 20
    ) -> None:
        """Initialize."""
        Thread.__init__(self)
        self._stop_publishing = False
        self._copy_interval = copy_interval
        self.spark_driver_logs_s3_bucket = spark_driver_logs_s3_bucket
        self.spark_driver_logs_s3_prefix = spark_driver_logs_s3_prefix
        self.local_spark_driver_logs_file = local_spark_driver_logs_file
        self._s3_client = boto3.client("s3")

    def run(self) -> None:
        """Publishes Spark driver logs to the given S3 URL.
        """
        log.info("Start copying Spark driver logs to S3.")

        while not self._stop_publishing:
            self._upload_spark_driver_logs()
            time.sleep(self._copy_interval)

        self._upload_spark_driver_logs()

        log.info("Finished copying Spark driver logs to S3.")

    def down(self) -> None:
        """Stops publishing Spark driver logs."""
        self._stop_publishing = True

    def _upload_spark_driver_logs(self) -> None:
        if os.path.exists(self.local_spark_driver_logs_file):
            s3_file_prefix = os.path.normpath(self.spark_driver_logs_s3_prefix +
                                              self.local_spark_driver_logs_file)
            self._s3_client.upload_file(self.local_spark_driver_logs_file,
                                        self.spark_driver_logs_s3_bucket, s3_file_prefix)
