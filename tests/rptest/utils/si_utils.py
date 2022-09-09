import collections
import json
import pprint
import random
import struct
from collections import defaultdict, namedtuple
from typing import Sequence

import confluent_kafka
import xxhash

from rptest.archival.s3_client import S3ObjectMetadata, S3Client
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST

EMPTY_SEGMENT_SIZE = 4096

BLOCK_SIZE = 4096

default_log_segment_size = 1048576  # 1MB

NTP = namedtuple("NTP", ['ns', 'topic', 'partition'])

TopicManifestMetadata = namedtuple('TopicManifestMetadata',
                                   ['ntp', 'revision'])
SegmentMetadata = namedtuple(
    'SegmentMetadata',
    ['ntp', 'revision', 'base_offset', 'term', 'md5', 'size'])

SegmentPathComponents = namedtuple('SegmentPathComponents',
                                   ['ntp', 'revision', 'name'])

ManifestPathComponents = namedtuple('ManifestPathComponents',
                                    ['ntp', 'revision'])

MISSING_DATA_ERRORS = [
    "No segments found. Empty partition manifest generated",
    "Error during log recovery: cloud_storage::missing_partition_exception",
    "Failed segment download",
]

TRANSIENT_ERRORS = RESTART_LOG_ALLOW_LIST + [
    "raft::offset_monitor::wait_aborted",
    "Upload loop error: seastar::timed_out_error"
]


class SegmentReader:
    HDR_FMT_RP = "<IiqbIhiqqqhii"
    HEADER_SIZE = struct.calcsize(HDR_FMT_RP)
    Header = collections.namedtuple(
        'Header', ('header_crc', 'batch_size', 'base_offset', 'type', 'crc',
                   'attrs', 'delta', 'first_ts', 'max_ts', 'producer_id',
                   'producer_epoch', 'base_seq', 'record_count'))

    def __init__(self, stream):
        self.stream = stream

    def read_batch(self):
        data = self.stream.read(self.HEADER_SIZE)
        if len(data) == self.HEADER_SIZE:
            header = self.Header(*struct.unpack(self.HDR_FMT_RP, data))
            if all(map(lambda v: v == 0, header)):
                return None
            records_size = header.batch_size - self.HEADER_SIZE
            data = self.stream.read(records_size)
            if len(data) < records_size:
                return None
            assert len(data) == records_size
            return header
        return None

    def __iter__(self):
        while True:
            it = self.read_batch()
            if it is None:
                return
            yield it


def parse_s3_manifest_path(path):
    """Parse S3 manifest path. Return ntp and revision.
    Sample name: 50000000/meta/kafka/panda-topic/0_19/manifest.json
    """
    items = path.split('/')
    ns = items[2]
    topic = items[3]
    part_rev = items[4].split('_')
    partition = int(part_rev[0])
    revision = int(part_rev[1])
    ntp = NTP(ns=ns, topic=topic, partition=partition)
    return ManifestPathComponents(ntp=ntp, revision=revision)


def parse_s3_segment_path(path):
    """Parse S3 segment path. Return ntp, revision and name.
    Sample name: b525cddd/kafka/panda-topic/0_9/4109-1-v1.log
    """
    items = path.split('/')
    ns = items[1]
    topic = items[2]
    part_rev = items[3].split('_')
    partition = int(part_rev[0])
    revision = int(part_rev[1])
    fname = items[4]
    ntp = NTP(ns=ns, topic=topic, partition=partition)
    return SegmentPathComponents(ntp=ntp, revision=revision, name=fname)


def _parse_checksum_entry(path, value, ignore_rev):
    """Parse output of the '_collect_file_checksums. Interprets path as a
    normalized path
    (e.g. <ns>/<topic>/<partition>_<revision>/<baseoffset>-<term>-v1.log).
    The value should contain a pair of md5 hash and file size."""
    md5, segment_size = value
    items = path.split('/')
    ns = items[0]
    topic = items[1]
    part_rev = items[2].split('_')
    partition = int(part_rev[0])
    revision = 0 if ignore_rev else int(part_rev[1])
    fname = items[3].split('-')
    base_offset = int(fname[0])
    term = int(fname[1])
    ntp = NTP(ns=ns, topic=topic, partition=partition)
    return SegmentMetadata(ntp=ntp,
                           revision=revision,
                           base_offset=base_offset,
                           term=term,
                           md5=md5,
                           size=segment_size)


def verify_file_layout(baseline_per_host,
                       restored_per_host,
                       expected_topics,
                       logger,
                       size_overrides=None):
    """This function checks the restored segments over the expected ones.
    It takes into account the fact that the md5 checksum as well as the
    file name of the restored segment might be different from the original
    segment. This is because we're deleting raft configuration batches
    from the segments.
    The function checks the size of the parition over the size of the original.
    The assertion is triggered only if the difference can't be explained by the
    upload lag and removal of configuration/archival-metadata batches.
    """

    if size_overrides is None:
        size_overrides = {}

    def get_ntp_sizes(fdata_per_host, hosts_can_vary=True):
        """Pre-process file layout data from the cluster. Input is a dictionary
        that maps host to dict of ntps where each ntp is mapped to the list of
        segments. The result is a map from ntp to the partition size on disk.
        """
        ntps = defaultdict(int)
        for _, fdata in fdata_per_host.items():
            ntp_size = defaultdict(int)
            for path, entry in fdata.items():
                it = _parse_checksum_entry(path, entry, ignore_rev=True)
                if it.ntp.topic in expected_topics:
                    if it.size > EMPTY_SEGMENT_SIZE:
                        # filter out empty segments created at the end of the log
                        # which are created after recovery
                        ntp_size[it.ntp] += it.size

            for ntp, total_size in ntp_size.items():
                if ntp in ntps and not hosts_can_vary:
                    # the size of the partition should be the
                    # same on every replica in the restored
                    # cluster
                    logger.info(
                        f"checking size of the partition for {ntp}, new {total_size} vs already accounted {ntps[ntp]}"
                    )
                    assert total_size == ntps[ntp]
                else:
                    ntps[ntp] = max(total_size, ntps[ntp])
        return ntps

    restored_ntps = get_ntp_sizes(restored_per_host, hosts_can_vary=False)
    baseline_ntps = get_ntp_sizes(baseline_per_host, hosts_can_vary=True)

    logger.info(f"before matching\n"
                f"restored ntps: {restored_ntps}\n"
                f"baseline ntps: {baseline_ntps}\n"
                f"expected topics: {expected_topics}")

    for ntp, orig_ntp_size in baseline_ntps.items():
        # Restored ntp should be equal or less than original
        # but not by much. It can be off by one segment size.
        # Also, each segment may lose a configuration batch or two.
        if ntp in size_overrides:
            logger.info(
                f"NTP {ntp} uses size override {orig_ntp_size} - {size_overrides[ntp]}"
            )
            orig_ntp_size -= size_overrides[ntp]
        assert ntp in restored_ntps, f"NTP {ntp} is missing in the restored data"
        rest_ntp_size = restored_ntps[ntp]
        assert rest_ntp_size <= orig_ntp_size, \
            f"NTP {ntp} the restored partition is larger {rest_ntp_size} than the original one {orig_ntp_size}."

        delta = orig_ntp_size - rest_ntp_size
        assert delta <= BLOCK_SIZE, \
            f"NTP {ntp} the restored partition is too small {rest_ntp_size}." \
            f" The original is {orig_ntp_size} bytes which {delta} bytes larger."


def gen_manifest_path(ntp, rev):
    x = xxhash.xxh32()
    path = f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{rev}"
    x.update(path.encode('ascii'))
    hash = x.hexdigest()[0] + '0000000'
    return f"{hash}/meta/{path}/manifest.json"


def _gen_segment_path(ntp, rev, name):
    x = xxhash.xxh32()
    path = f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{rev}/{name}"
    x.update(path.encode('ascii'))
    hash = x.hexdigest()
    return f"{hash}/{path}"


def get_on_disk_size_per_ntp(chk):
    """Get number of bytes used per ntp"""
    size_bytes_per_ntp = {}
    for _, data in chk.items():
        tmp_size = defaultdict(int)
        for path, summary in data.items():
            segment = _parse_checksum_entry(path, summary, True)
            ntp = segment.ntp
            size = summary[1]
            tmp_size[ntp] += size
        for ntp, size in tmp_size.items():
            if not ntp in size_bytes_per_ntp or size_bytes_per_ntp[ntp] < size:
                size_bytes_per_ntp[ntp] = size
    return size_bytes_per_ntp


def is_close_size(actual_size, expected_size):
    """Checks if the log size is close to expected size.
    The actual size shouldn't be less than expected. Also, the difference
    between two values shouldn't be greater than the size of one segment.
    It also takes into account segment size jitter.
    """
    lower_bound = expected_size
    upper_bound = expected_size + default_log_segment_size + \
                  int(default_log_segment_size * 0.2)
    return actual_size in range(lower_bound, upper_bound)


class PathMatcher:
    def __init__(self, expected_topics: Sequence[TopicSpec]):
        self.expected_topics = expected_topics
        self.topic_names = {t.name for t in self.expected_topics}
        self.topic_manifest_paths = {
            f'/{t}/topic_manifest.json'
            for t in self.topic_names
        }

    def is_partition_manifest(self, o: S3ObjectMetadata) -> bool:
        return o.Key.endswith('/manifest.json') and any(
            tn in o.Key for tn in self.topic_names)

    def is_topic_manifest(self, o: S3ObjectMetadata) -> bool:
        return any(o.Key.endswith(t) for t in self.topic_manifest_paths)

    def is_segment(self, o: S3ObjectMetadata) -> bool:
        try:
            return parse_s3_segment_path(o.Key).ntp.topic in self.topic_names
        except Exception:
            return False

    def path_matches_any_topic(self, path: str) -> bool:
        return any(t in path for t in self.topic_names)


class Producer:
    def __init__(self, brokers, name, logger, timeout_sec: float = 60.0):
        self.keys = []
        self.cur_offset = 0
        self.brokers = brokers
        self.logger = logger
        self.num_aborted = 0
        self.name = name
        self.timeout_sec = timeout_sec
        self.reconnect()

    def reconnect(self):
        self.producer = confluent_kafka.Producer({
            'bootstrap.servers':
            self.brokers,
            'transactional.id':
            self.name,
            'transaction.timeout.ms':
            5000,
        })
        self.producer.init_transactions(self.timeout_sec)

    def produce(self, topic):
        """produce some messages inside a transaction with increasing keys
        and random values. Then randomly commit/abort the transaction."""

        n_msgs = random.randint(50, 100)
        keys = []

        self.producer.begin_transaction()
        for _ in range(n_msgs):
            val = ''.join(
                map(chr, (random.randint(0, 256)
                          for _ in range(random.randint(100, 1000)))))
            self.producer.produce(topic, val, str(self.cur_offset))
            keys.append(str(self.cur_offset).encode('utf8'))
            self.cur_offset += 1

        self.logger.info(f"writing {len(keys)} msgs: {keys[0]}-{keys[-1]}...")
        self.producer.flush()
        if random.random() < 0.1:
            self.producer.abort_transaction()
            self.num_aborted += 1
            self.logger.info("aborted txn")
        else:
            self.producer.commit_transaction()
            self.keys.extend(keys)


class S3View:
    def __init__(self, expected_topics: Sequence[TopicSpec], client: S3Client,
                 bucket: str, logger):
        self.logger = logger
        self.bucket = bucket
        self.client = client
        self.expected_topics = expected_topics
        self.path_matcher = PathMatcher(self.expected_topics)
        self.objects = self.client.list_objects(self.bucket)
        self.partition_manifests = {}
        for o in self.objects:
            if self.path_matcher.is_partition_manifest(o):
                manifest_path = parse_s3_manifest_path(o.Key)
                data = self.client.get_object_data(self.bucket, o.Key)
                self.partition_manifests[manifest_path.ntp] = json.loads(data)
                self.logger.debug(
                    f'registered partition manifest for {manifest_path.ntp}: '
                    f'{pprint.pformat(self.partition_manifests[manifest_path.ntp], indent=2)}'
                )

    def is_segment_part_of_a_manifest(self, o: S3ObjectMetadata) -> bool:
        """
        Queries that given object is a segment, and is a part of one of the test partition manifests
        with a matching archiver term
        """
        try:
            if not self.path_matcher.is_segment(o):
                return False

            segment_path = parse_s3_segment_path(o.Key)
            partition_manifest = self.partition_manifests.get(segment_path.ntp)
            if not partition_manifest:
                self.logger.warn(f'no manifest found for {segment_path.ntp}')
                return False
            segments_in_manifest = partition_manifest['segments']

            # Filename for segment contains the archiver term, eg:
            # 4886-1-v1.log.2 -> 4886-1-v1.log and 2
            base_name, archiver_term = segment_path.name.rsplit('.', 1)
            segment_entry = segments_in_manifest.get(base_name)
            if not segment_entry:
                self.logger.warn(
                    f'no entry found for segment path {base_name} '
                    f'in manifest: {pprint.pformat(segments_in_manifest, indent=2)}'
                )
                return False

            # Archiver term should match the value in partition manifest
            manifest_archiver_term = str(segment_entry['archiver_term'])
            if archiver_term == manifest_archiver_term:
                return True

            self.logger.warn(
                f'{segment_path} has archiver term {archiver_term} '
                f'which does not match manifest term {manifest_archiver_term}')
            return False
        except Exception as e:
            self.logger.info(f'error {e} while checking if {o} is a segment')
            return False
