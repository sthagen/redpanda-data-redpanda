import boto3
import re
from botocore.config import Config
from botocore.exceptions import ClientError
from time import sleep
from functools import wraps
import time
import datetime
from typing import Iterator, NamedTuple, Union, Optional


class SlowDown(Exception):
    pass


class S3ObjectMetadata(NamedTuple):
    # TODO change to snake_case
    Bucket: str
    Key: str
    ETag: str
    ContentLength: int


def retry_on_slowdown(tries=4, delay=1.0, backoff=2.0):
    """Retry with an exponential backoff if SlowDown exception was triggered."""
    def retry(fn):
        @wraps(fn)
        def do_retry(*args, **kwargs):
            ntries, sec_delay = tries, delay
            while ntries > 1:
                try:
                    return fn(*args, **kwargs)
                except SlowDown:
                    sleep(sec_delay)
                    ntries -= 1
                    sec_delay *= backoff
            # will stop filtering SlowDown exception when quota is reache
            return fn(*args, **kwargs)

        return do_retry

    return retry


class S3Client:
    """Simple S3 client"""
    def __init__(self,
                 region,
                 access_key,
                 secret_key,
                 logger,
                 endpoint=None,
                 disable_ssl=True):
        cfg = Config(region_name=region, signature_version='s3v4')
        self._cli = boto3.client('s3',
                                 config=cfg,
                                 aws_access_key_id=access_key,
                                 aws_secret_access_key=secret_key,
                                 endpoint_url=endpoint,
                                 use_ssl=not disable_ssl)
        self._region = region
        self.logger = logger

    def create_bucket(self, name):
        """Create bucket in S3"""
        try:
            res = self._cli.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={'LocationConstraint': self._region})

            self.logger.debug(res)
            assert res['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as err:
            if err.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise err

    def delete_bucket(self, name):
        try:
            self._cli.delete_bucket(Bucket=name)
        except Exception:
            self.logger.warn("Error deleting bucket, contents:")
            for o in self.list_objects(name):
                self.logger.warn(f"  {o.Key}")
            raise

    def empty_bucket(self, name):
        """Empty bucket, return list of keys that wasn't deleted due
        to error"""
        keys = []
        try:
            self.logger.debug(f"running bucket cleanup on {name}")
            for obj in self.list_objects(bucket=name):
                keys.append(obj.Key)
                self.logger.debug(f"found key {obj.Key}")
        except Exception as e:
            # Expected to fail if bucket doesn't exist
            self.logger.debug(f"empty_bucket error: {e}")

        failed_keys = []
        while len(keys):
            # S3 API supports up to 1000 keys per delete request
            batch = keys[0:1000]
            keys = keys[1000:]
            self.logger.debug(f"deleting keys {batch[0]}..{batch[-1]}")
            try:
                reply = self._cli.delete_objects(
                    Bucket=name,
                    Delete={'Objects': [{
                        'Key': k
                    } for k in batch]})
                self.logger.debug(f"delete request reply: {reply}")
            except:
                self.logger.exception(
                    f"Delete request failed for keys {batch[0]}..{batch[-1]}")
                failed_keys.extend(batch)
        return failed_keys

    def delete_object(self, bucket, key, verify=False):
        """Remove object from S3"""
        res = self._delete_object(bucket, key)
        if verify:
            self._wait_no_key(bucket, key)
        return res

    def _wait_no_key(self, bucket, key, timeout_sec=10):
        """Wait for the key to apper in the bucket"""
        deadline = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_sec)
        try:
            # Busy wait until the object is actually
            # deleted
            while True:
                self._head_object(bucket, key)
                # aws boto3 uses 5s interval for polling
                # using head_object API call
                now = datetime.datetime.now()
                if now > deadline:
                    raise TimeoutError()
                time.sleep(5)
        except ClientError as err:
            self.logger.debug(f"error response while polling {err}")

    def _wait_key(self, bucket, key, timeout_sec=30):
        """Wait for the key to apper in the bucket"""
        # Busy wait until the object is available
        deadline = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_sec)
        while True:
            try:
                meta = self._head_object(bucket, key)
                self.logger.debug(f"object {key} is available, head: {meta}")
                return
            except ClientError as err:
                self.logger.debug(f"error response while polling {err}")
            now = datetime.datetime.now()
            if now > deadline:
                raise TimeoutError()
            # aws boto3 uses 5s interval for polling
            # using head_object API call
            time.sleep(5)

    @retry_on_slowdown()
    def _delete_object(self, bucket, key):
        """Remove object from S3"""
        try:
            return self._cli.delete_object(Bucket=bucket, Key=key)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _get_object(self, bucket, key):
        """Get object from S3"""
        try:
            return self._cli.get_object(Bucket=bucket, Key=key)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _head_object(self, bucket, key):
        """Get object from S3"""
        try:
            return self._cli.head_object(Bucket=bucket, Key=key)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _put_object(self, bucket, key, content):
        """Put object to S3"""
        try:
            return self._cli.put_object(Bucket=bucket,
                                        Key=key,
                                        Body=bytes(content, encoding='utf-8'))
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _copy_single_object(self, bucket, src, dst):
        """Copy object to another location within the bucket"""
        try:
            src_uri = f"{bucket}/{src}"
            return self._cli.copy_object(Bucket=bucket,
                                         Key=dst,
                                         CopySource=src_uri)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    def get_object_data(self, bucket, key):
        resp = self._get_object(bucket, key)
        return resp['Body'].read()

    def put_object(self, bucket, key, data):
        self._put_object(bucket, key, data)

    def copy_object(self,
                    bucket,
                    src,
                    dst,
                    validate=False,
                    validation_timeout_sec=30):
        """Copy object inside a bucket and optionally poll the destination until
        the copy will be available or timeout passes."""
        self._copy_single_object(bucket, src, dst)
        if validate:
            self._wait_key(bucket, dst, validation_timeout_sec)

    def move_object(self,
                    bucket,
                    src,
                    dst,
                    validate=False,
                    validation_timeout_sec=30):
        """Move object inside a bucket and optionally poll the destination until
        the new location will be available and old will disappear or timeout is reached."""
        self._copy_single_object(bucket, src, dst)
        self._delete_object(bucket, src)
        if validate:
            self._wait_key(bucket, dst, validation_timeout_sec)
            self._wait_no_key(bucket, src, validation_timeout_sec)

    def get_object_meta(self, bucket, key):
        """Get object metadata without downloading it"""
        resp = self._get_object(bucket, key)
        # Note: ETag field contains md5 hash enclosed in double quotes that have to be removed
        return S3ObjectMetadata(Bucket=bucket,
                                Key=key,
                                ETag=resp['ETag'][1:-1],
                                ContentLength=resp['ContentLength'])

    def write_object_to_file(self, bucket, key, dest_path):
        """Get object and write it to file"""
        resp = self._get_object(bucket, key)
        with open(dest_path, 'wb') as f:
            body = resp['Body']
            for chunk in body.iter_chunks(chunk_size=0x1000):
                f.write(chunk)

    @retry_on_slowdown()
    def _list_objects(self, bucket, token=None, limit=1000):
        try:
            if token is not None:
                return self._cli.list_objects_v2(Bucket=bucket,
                                                 MaxKeys=limit,
                                                 ContinuationToken=token)
            else:
                return self._cli.list_objects_v2(Bucket=bucket, MaxKeys=limit)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    def _key_to_topic(self, key: str):
        # Segment objects: <hash>/<ns>/<topic>/<partition>_<revision>/...
        # Manifest objects: <hash>/meta/<ns>/<topic>/<partition>_<revision>/...
        # Topic manifest objects: <hash>/meta/<ns>/<topic>/topic_manifest.json
        m = re.search(".+/(.+)/(.+)/(\d+_\d+/|topic_manifest.json)", key)
        if m is None:
            return None
        else:
            return m.group(2)

    def list_objects(
            self,
            bucket,
            topic: Optional[str] = None) -> Iterator[S3ObjectMetadata]:
        """
        :param bucket: S3 bucket name
        :param topic: Optional, if set then only return objects belonging to this topic
        """
        token = None
        truncated = True
        while truncated:
            res = self._list_objects(bucket, token, limit=100)
            token = res.get('NextContinuationToken')
            truncated = bool(res['IsTruncated'])
            if 'Contents' in res:
                for item in res['Contents']:

                    # Apply optional topic filtering
                    if topic is not None and self._key_to_topic(
                            item['Key']) != topic:
                        self.logger.debug(f"Skip {item['Key']} for {topic}")
                        continue

                    yield S3ObjectMetadata(Bucket=bucket,
                                           Key=item['Key'],
                                           ETag=item['ETag'][1:-1],
                                           ContentLength=item['Size'])

    def list_buckets(self) -> dict[str, Union[list, dict]]:
        try:
            return self._cli.list_buckets()
        except Exception as ex:
            self.logger.error(f'Error listing buckets: {ex}')
            raise
