from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
import pandas as pd
from .base import DataWriter
from utils.logger import logger
from datetime import datetime, timezone, timedelta

class InfluxdbWriter(DataWriter):
    def __init__(self, 
                 url: str,
                 token: str,
                 org: str,
                 bucket: str,
                 measurement: str,
                 timestamp_col: str,
                 tag_cols: list[str],
                 field_cols: list[str],
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.measurement = measurement
        self.timestamp_col = timestamp_col
        self.tag_cols = tag_cols
        self.field_cols = field_cols
        # fine-tune the influxdb writer for better performance
        # fine-tune the batch size and flush interval
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        flush_interval = kwargs.get("flush_interval", 1000)
        batch_size = kwargs.get("batch_size", 1000)
        self.write_persion = kwargs.get("write_precision", WritePrecision.NS)
        self.write_api = self.client.write_api(write_options=WriteOptions(batch_size=batch_size, flush_interval=flush_interval))
        # setting the influxdb bucket and organization
        self.setup_influxdb_org_bucket(org_name=org, bucket_name=bucket)
        logger.info(f"InfluxdbWriter: Initialized with url={self.url}, org={self.org.name}, bucket={self.bucket.name}, measurement={self.measurement}")

    def setup_influxdb_org_bucket(self, org_name: str, bucket_name: str) -> None:
        """
        Setup the InfluxDB client and write API. Check if the bucket and organization exist; create them if they don't.
        """
        # Check if the organization exists
        orgs = self.client.organizations_api().find_organizations()
        if not any(org.name == org_name for org in orgs):
            # Create the organization
            logger.warning(f"InfluxdbWriter: Organization {org_name} does not exist. Creating it.")
            org = self.client.organizations_api().create_organization(name=org_name)
            self.org = org
            self.org_id = org.id
        else:
            # find the organization ID in orgs
            orgs = self.client.organizations_api().find_organizations()
            self.org = next(org for org in orgs if org.name == org_name)
            self.org_id = self.org.id
        # Check if the bucket exists
        if not self.client.buckets_api().find_bucket_by_name(self.bucket):
            # Create the bucket
            logger.warning(f"InfluxdbWriter: Bucket {self.bucket} does not exist. Creating it.")
            bucket = self.client.buckets_api().create_bucket(bucket_name=self.bucket, org_id=self.org_id)
            self.bucket = bucket
            self.bucket_id = bucket.id
        else:
            # find the bucket ID
            bucket = self.client.buckets_api().find_bucket_by_name(self.bucket)
            self.bucket = bucket
            self.bucket_id = bucket.id

    def write(self, data: pd.DataFrame) -> None:
        # df = data.copy(deep=True)
        df = data
        # Ensure the timestamp column is of integer or datetime type
        if not pd.api.types.is_integer_dtype(df[self.timestamp_col]) and not pd.api.types.is_datetime64_any_dtype(df[self.timestamp_col]):
            logger.debug(f"InfluxdbWriter: Timestamp column {self.timestamp_col} is not integer or datetime. Setting it to now.")
            df[self.timestamp_col] = pd.Timestamp.now()
        # Convert datetime to int epoch time if necessary
        if pd.api.types.is_datetime64_any_dtype(df[self.timestamp_col]):
            if df[self.timestamp_col].dt.tz is None:
                local_offset = -1 * datetime.now().astimezone().utcoffset()
                df[self.timestamp_col] = df[self.timestamp_col].map(lambda x: x + local_offset)
                df[self.timestamp_col].dt.tz_localize('UTC')
            df[self.timestamp_col] = df[self.timestamp_col].astype('int64')
        # convert the dataframe to a list of InfluxDB points
        points = [Point.from_dict({
            "measurement": self.measurement,
            "tags": {col: row[col] for col in self.tag_cols},
            "fields": {col: row[col] for col in self.field_cols},
            "time": row[self.timestamp_col]
        }, self.write_persion) for _, row in df.iterrows()]
        # write the points to InfluxDB
        self.write_api.write(bucket=self.bucket.name, org=self.org.name, record=points)
        logger.debug(f"InfluxdbWriter: Wrote {len(points)} points to InfluxDB")
    
    def close(self) -> None:
        logger.info("InfluxdbWriter: Closing InfluxDB client")
        self.client.close()

    @staticmethod
    def required_parameters() -> dict[str, str]:
        return {
            "url": "str",
            "token": "str",
            "org": "str",
            "bucket": "str",
            "measurement": "str",
            "timestamp_col": "str",
            "tag_cols": "list[str]",
            "field_cols": "list[str]"
        }

    @staticmethod
    def from_config(config: dict) -> 'DataWriter':
        return InfluxdbWriter(**config)