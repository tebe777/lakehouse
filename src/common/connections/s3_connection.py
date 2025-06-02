# src/common/connections/s3_connection.py

import boto3
import os
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import logging
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
import structlog

logger = structlog.get_logger(__name__)


class S3ConnectionManager:
    """
    Connection manager for S3-compatible storage (MinIO, AWS S3, etc.).
    Handles boto3 client creation, configuration, and common operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize S3 connection manager.
        
        Args:
            config: Configuration dictionary with connection parameters
        """
        self.config = config
        self.logger = logger.bind(component="S3ConnectionManager")
        
        # Extract connection parameters
        self.endpoint_url = config.get('endpoint')
        self.access_key = config.get('access_key')
        self.secret_key = config.get('secret_key')
        self.region = config.get('region', 'us-east-1')
        self.use_ssl = config.get('use_ssl', True)
        self.verify_ssl = config.get('verify_ssl', True)
        
        # Client configuration
        self.client_config = Config(
            retries={'max_attempts': config.get('max_retries', 3)},
            connect_timeout=config.get('connect_timeout', 60),
            read_timeout=config.get('read_timeout', 300)
        )
        
        # Initialize clients
        self._s3_client = None
        self._s3_resource = None
        
        self.logger.info("S3ConnectionManager initialized", 
                        endpoint=self.endpoint_url,
                        region=self.region)
    
    @property
    def s3_client(self):
        """Get or create S3 client."""
        if self._s3_client is None:
            self._s3_client = self._create_s3_client()
        return self._s3_client
    
    @property
    def s3_resource(self):
        """Get or create S3 resource."""
        if self._s3_resource is None:
            self._s3_resource = self._create_s3_resource()
        return self._s3_resource
    
    def _create_s3_client(self):
        """Create boto3 S3 client."""
        try:
            client_params = {
                'service_name': 's3',
                'endpoint_url': self.endpoint_url,
                'aws_access_key_id': self.access_key,
                'aws_secret_access_key': self.secret_key,
                'region_name': self.region,
                'config': self.client_config,
                'use_ssl': self.use_ssl,
                'verify': self.verify_ssl
            }
            
            client = boto3.client(**client_params)
            
            # Test connection
            client.list_buckets()
            self.logger.info("S3 client created successfully")
            
            return client
            
        except Exception as e:
            self.logger.error("Failed to create S3 client", error=str(e))
            raise ConnectionError(f"Failed to create S3 client: {str(e)}")
    
    def _create_s3_resource(self):
        """Create boto3 S3 resource."""
        try:
            resource_params = {
                'service_name': 's3',
                'endpoint_url': self.endpoint_url,
                'aws_access_key_id': self.access_key,
                'aws_secret_access_key': self.secret_key,
                'region_name': self.region,
                'config': self.client_config,
                'use_ssl': self.use_ssl,
                'verify': self.verify_ssl
            }
            
            resource = boto3.resource(**resource_params)
            
            self.logger.info("S3 resource created successfully")
            return resource
            
        except Exception as e:
            self.logger.error("Failed to create S3 resource", error=str(e))
            raise ConnectionError(f"Failed to create S3 resource: {str(e)}")
    
    def test_connection(self) -> bool:
        """Test S3 connection."""
        try:
            response = self.s3_client.list_buckets()
            bucket_count = len(response.get('Buckets', []))
            self.logger.info("S3 connection test successful", bucket_count=bucket_count)
            return True
        except Exception as e:
            self.logger.error("S3 connection test failed", error=str(e))
            return False
    
    def list_buckets(self) -> List[str]:
        """List all available buckets."""
        try:
            response = self.s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
            self.logger.info("Listed buckets", bucket_count=len(buckets))
            return buckets
        except Exception as e:
            self.logger.error("Failed to list buckets", error=str(e))
            raise
    
    def list_objects(self, bucket: str, prefix: str = "", max_keys: int = 1000) -> List[Dict[str, Any]]:
        """
        List objects in bucket with optional prefix.
        
        Args:
            bucket: Bucket name
            prefix: Object prefix filter
            max_keys: Maximum number of objects to return
            
        Returns:
            List of object metadata dictionaries
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            page_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=prefix,
                PaginationConfig={'MaxItems': max_keys}
            )
            
            objects = []
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    objects.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag']
                    })
            
            self.logger.info("Listed objects", 
                           bucket=bucket, 
                           prefix=prefix, 
                           object_count=len(objects))
            return objects
            
        except Exception as e:
            self.logger.error("Failed to list objects", 
                            bucket=bucket, 
                            prefix=prefix, 
                            error=str(e))
            raise
    
    def download_file(self, bucket: str, key: str, local_path: str) -> bool:
        """
        Download file from S3 to local path.
        
        Args:
            bucket: Bucket name
            key: Object key
            local_path: Local file path to save to
            
        Returns:
            True if successful
        """
        try:
            # Ensure local directory exists
            local_path_obj = Path(local_path)
            local_path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            # Download file
            self.s3_client.download_file(bucket, key, str(local_path_obj))
            
            # Verify file was downloaded
            if local_path_obj.exists() and local_path_obj.stat().st_size > 0:
                self.logger.info("File downloaded successfully", 
                               bucket=bucket, 
                               key=key, 
                               local_path=local_path,
                               file_size=local_path_obj.stat().st_size)
                return True
            else:
                self.logger.error("Downloaded file is empty or missing", 
                                bucket=bucket, 
                                key=key, 
                                local_path=local_path)
                return False
                
        except Exception as e:
            self.logger.error("Failed to download file", 
                            bucket=bucket, 
                            key=key, 
                            local_path=local_path, 
                            error=str(e))
            raise
    
    def download_files_by_prefix(self, bucket: str, prefix: str, local_dir: str, 
                               file_pattern: Optional[str] = None) -> List[str]:
        """
        Download multiple files matching prefix to local directory.
        
        Args:
            bucket: Bucket name
            prefix: Prefix to filter objects
            local_dir: Local directory to download to
            file_pattern: Optional pattern to filter filenames (e.g., "*.csv")
            
        Returns:
            List of downloaded file paths
        """
        try:
            # List objects with prefix
            objects = self.list_objects(bucket, prefix)
            
            # Filter by pattern if provided
            if file_pattern:
                import fnmatch
                objects = [obj for obj in objects if fnmatch.fnmatch(obj['key'], file_pattern)]
            
            downloaded_files = []
            local_dir_path = Path(local_dir)
            local_dir_path.mkdir(parents=True, exist_ok=True)
            
            for obj in objects:
                key = obj['key']
                filename = Path(key).name
                local_file_path = local_dir_path / filename
                
                # Download file
                if self.download_file(bucket, key, str(local_file_path)):
                    downloaded_files.append(str(local_file_path))
            
            self.logger.info("Downloaded files by prefix", 
                           bucket=bucket, 
                           prefix=prefix, 
                           downloaded_count=len(downloaded_files))
            
            return downloaded_files
            
        except Exception as e:
            self.logger.error("Failed to download files by prefix", 
                            bucket=bucket, 
                            prefix=prefix, 
                            error=str(e))
            raise
    
    def upload_file(self, local_path: str, bucket: str, key: str) -> bool:
        """
        Upload local file to S3.
        
        Args:
            local_path: Local file path
            bucket: Target bucket name
            key: Target object key
            
        Returns:
            True if successful
        """
        try:
            local_path_obj = Path(local_path)
            
            if not local_path_obj.exists():
                raise FileNotFoundError(f"Local file not found: {local_path}")
            
            # Upload file
            self.s3_client.upload_file(str(local_path_obj), bucket, key)
            
            self.logger.info("File uploaded successfully", 
                           local_path=local_path, 
                           bucket=bucket, 
                           key=key,
                           file_size=local_path_obj.stat().st_size)
            return True
            
        except Exception as e:
            self.logger.error("Failed to upload file", 
                            local_path=local_path, 
                            bucket=bucket, 
                            key=key, 
                            error=str(e))
            raise
    
    def get_object_metadata(self, bucket: str, key: str) -> Dict[str, Any]:
        """
        Get object metadata without downloading.
        
        Args:
            bucket: Bucket name
            key: Object key
            
        Returns:
            Object metadata dictionary
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            
            metadata = {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'etag': response['ETag'],
                'content_type': response.get('ContentType'),
                'metadata': response.get('Metadata', {})
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error("Failed to get object metadata", 
                            bucket=bucket, 
                            key=key, 
                            error=str(e))
            raise
    
    def object_exists(self, bucket: str, key: str) -> bool:
        """Check if object exists in bucket."""
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise
    
    def get_presigned_url(self, bucket: str, key: str, expiration: int = 3600) -> str:
        """
        Generate presigned URL for object access.
        
        Args:
            bucket: Bucket name
            key: Object key
            expiration: URL expiration time in seconds
            
        Returns:
            Presigned URL string
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': key},
                ExpiresIn=expiration
            )
            
            self.logger.info("Generated presigned URL", 
                           bucket=bucket, 
                           key=key, 
                           expiration=expiration)
            return url
            
        except Exception as e:
            self.logger.error("Failed to generate presigned URL", 
                            bucket=bucket, 
                            key=key, 
                            error=str(e))
            raise
    
    def close(self):
        """Close connections and cleanup resources."""
        self._s3_client = None
        self._s3_resource = None
        self.logger.info("S3 connections closed")


def create_s3_connection_from_config(config: Dict[str, Any]) -> S3ConnectionManager:
    """
    Factory function to create S3ConnectionManager from configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        S3ConnectionManager instance
    """
    return S3ConnectionManager(config) 