from typing import Optional, List
from enum import Enum, auto
import boto3
from botocore.exceptions import ClientError
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from utils.db_client import DatabaseClient
import uuid
from datetime import datetime
import logging
import os
from urllib.parse import quote_plus

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, continue without it

logger = logging.getLogger(__name__)


class RunType(str, Enum):
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    DATE_RANGE = "DATE_RANGE"
    ORCHESTRATED = "ORCHESTRATED"


class ProcessingType(str, Enum):
    BATCH = "BATCH"
    STREAMING = "STREAMING"


class AppSettings(BaseSettings):
    class Config:
        case_sensitive = False

    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_ts: str = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    project_name: str = Field(default="ws-analytics")

    # Processing settings
    run_type: RunType = Field(default=RunType.INCREMENTAL)
    processing_type: ProcessingType = Field(default=ProcessingType.BATCH)
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)
    scrape_run_id: Optional[str] = Field(default=None)
    
    # @field_validator('scrape_run_id', mode='before')
    # @classmethod
    # def parse_scrape_run_id(cls, v):
    #     """Convert string to list if needed"""
    #     if isinstance(v, str):
    #         return [v]
    #     return v
    
    # Performance settings
    max_keys_per_unit: int = Field(default=10)
    max_workers: Optional[int] = Field(default=None)
    chunk_size: int = Field(default=1000)
    batch_size: int = Field(default=100)

    def _get_ssm_parameter(self, name: str) -> Optional[str]:
        if not self._ssm_client:
            return None
        try:
            response = self._ssm_client.get_parameter(Name=name, WithDecryption=True)
            return response.get("Parameter", {}).get("Value")
        except (ClientError, self._ssm_client.exceptions.ParameterNotFound):
            logger.warning(f"Parameter {name} not found in SSM.")
            return None

    @property
    def _ssm_client(self):
        if not hasattr(self, "__ssm_client"):
            try:
                region = os.environ.get("AWS_REGION", "us-east-1")
                self.__ssm_client = boto3.client("ssm", region_name=region)
            except ClientError as e:
                logger.warning(f"Could not create Boto3 SSM client. Error: {e}")
                self.__ssm_client = None
        return self.__ssm_client

    @property
    def database_client(self) -> DatabaseClient:
        if not hasattr(self, "_database_client"):
            ssm_path_prefix = f"/{self.project_name}/database"
            db_user = self._get_ssm_parameter(ssm_path_prefix + "/username")
            db_password = self._get_ssm_parameter(ssm_path_prefix + "/password")
            db_host = self._get_ssm_parameter(ssm_path_prefix + "/host")
            db_name = self._get_ssm_parameter(ssm_path_prefix + "/database")
            encoded_password = quote_plus(db_password)
            db_url = f"redshift+redshift_connector://{db_user}:{encoded_password}@{db_host}:5439/{db_name}"
            self._database_client = DatabaseClient(db_url)
        return self._database_client

    @property
    def s3_bucket(self) -> Optional[str]:
        if not hasattr(self, "_s3_bucket"):
            ssm_path = f"/{self.project_name}/s3/analytics/name"
            self._s3_bucket = self._get_ssm_parameter(ssm_path)
        return self._s3_bucket

    @property
    def s3(self) -> boto3.client:
        if not hasattr(self, "_s3_client"):
            region = os.environ.get("AWS_REGION", "us-east-1")
            session = boto3.Session(region_name=region)
            self._s3_client = session.client("s3")
        return self._s3_client

    @property
    def redshift_copy_role_arn(self) -> Optional[str]:
        if not hasattr(self, "_redshift_copy_role_arn"):
            ssm_path = f"/{self.project_name}/redshift/copy-role-arn"
            self._redshift_copy_role_arn = self._get_ssm_parameter(ssm_path)
        return self._redshift_copy_role_arn

    def object_exists(self, bucket_name: str, key: str) -> bool:
        """Check if an object exists in AWS S3 bucket"""
        try:
            self.s3.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise
    
    def close(self):
        """Close database connections"""
        if hasattr(self, "_database_client"):
            self._database_client.close()

    def refresh_database_connection(self):
        """Refresh database connection by disposing old client and forcing new connection"""
        if hasattr(self, "_database_client"):
            logger.info("Refreshing database connection (disposing old client)")
            self._database_client.close()
            delattr(self, "_database_client")
        logger.info("Database connection refreshed - new client will be created on next access")

    def validate_run_type(self):
        """Validate run type specific requirements"""
        if self.run_type == RunType.DATE_RANGE and (self.start_date is None or self.end_date is None):
            raise ValueError("Start date and end date must be provided for DATE_RANGE run type")
        
        if self.run_type == RunType.ORCHESTRATED and not self.scrape_run_id:
            logger.info("ORCHESTRATED run type without specific scrape_run_id - will process all unprocessed runs")

    def get_processing_config(self) -> dict:
        """Get processing configuration based on current settings"""
        return {
            "run_id": self.run_id,
            "run_type": self.run_type,
            "processing_type": self.processing_type,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "scrape_run_id": self.scrape_run_id,
            "max_keys_per_unit": self.max_keys_per_unit,
            "max_workers": self.max_workers,
            "chunk_size": self.chunk_size,
            "batch_size": self.batch_size,
            "s3_bucket": self.s3_bucket,
        }


settings = AppSettings()
