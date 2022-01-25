import collections
import os

import struct
import crc32c
import glob
import re
import logging
from io import BytesIO
from reader import Reader

logger = logging.getLogger('rp')

# https://docs.python.org/3.8/library/struct.html#format-strings
#
# redpanda header prefix:
#   - little endian encoded
#   - batch size, base offset, type crc
#
# note that the crc that is stored is the crc reported by kafka which happens to
# be computed over the big endian encoding of the same data. thus to verify the
# crc we need to rebuild part of the header in big endian before adding to crc.
HDR_FMT_RP_PREFIX_NO_CRC = "iqbI"
HDR_FMT_RP_PREFIX = "<I" + HDR_FMT_RP_PREFIX_NO_CRC

# below the crc redpanda and kafka have the same layout
#   - little endian encoded
#   - attributes ... record_count
HDR_FMT_CRC = "hiqqqhii"

HDR_FMT_RP = HDR_FMT_RP_PREFIX + HDR_FMT_CRC
HEADER_SIZE = struct.calcsize(HDR_FMT_RP)

Header = collections.namedtuple(
    'Header', ('header_crc', 'batch_size', 'base_offset', 'type', 'crc',
               'attrs', 'delta', 'first_ts', 'max_ts', 'producer_id',
               'producer_epoch', 'base_seq', 'record_count'))

SEGMENT_NAME_PATTERN = re.compile(
    "(?P<base_offset>\d+)-(?P<term>\d+)-v(?P<version>\d)\.log")


class CorruptBatchError(Exception):
    def __init__(self, batch):
        self.batch = batch


class Record:
    def __init__(self, length, attrs, timestamp_delta, offset_delta, key,
                 value, headers):
        self.length = length
        self.attrs = attrs
        self.timestamp_delta = timestamp_delta
        self.offset_delta = offset_delta
        self.key = key
        self.value = value
        self.headers = headers


class RecordHeader:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class RecordIter:
    def __init__(self, record_count, records_data):
        self.data_stream = BytesIO(records_data)
        self.rdr = Reader(self.data_stream)
        self.record_count = record_count

    def _parse_header(self):
        k_sz = self.rdr.read_varint()
        key = self.rdr.read_bytes(k_sz)
        v_sz = self.rdr.read_varint()
        value = self.rdr.read_bytes(v_sz)
        return RecordHeader(key, value)

    def __next__(self):
        if self.record_count == 0:
            raise StopIteration()

        self.record_count -= 1
        len = self.rdr.read_varint()
        attrs = self.rdr.read_int8()
        timestamp_delta = self.rdr.read_varint()
        offset_delta = self.rdr.read_varint()
        key_length = self.rdr.read_varint()
        if key_length > 0:
            key = self.rdr.read_bytes(key_length)
        else:
            key = None
        value_length = self.rdr.read_varint()
        if value_length > 0:
            value = self.rdr.read_bytes(value_length)
        else:
            value = None
        hdr_size = self.rdr.read_varint()
        headers = []
        for i in range(0, hdr_size):
            headers.append(self._parse_header())

        return Record(len, attrs, timestamp_delta, offset_delta, key, value,
                      headers)


class Batch:
    def __init__(self, index, header, records):
        self.index = index
        self.header = header
        self.term = None
        self.records = records

        header_crc_bytes = struct.pack(
            "<" + HDR_FMT_RP_PREFIX_NO_CRC + HDR_FMT_CRC, *self.header[1:])
        header_crc = crc32c.crc32c(header_crc_bytes)
        if self.header.header_crc != header_crc:
            raise CorruptBatchError(self)
        crc = crc32c.crc32c(self._crc_header_be_bytes())
        crc = crc32c.crc32c(records, crc)
        if self.header.crc != crc:
            raise CorruptBatchError(self)

    def last_offset(self):
        return self.header.base_offset + self.header.record_count - 1

    def _crc_header_be_bytes(self):
        # encode header back to big-endian for crc calculation
        return struct.pack(">" + HDR_FMT_CRC, *self.header[5:])

    @staticmethod
    def from_stream(f, index):
        data = f.read(HEADER_SIZE)
        if len(data) == HEADER_SIZE:
            header = Header(*struct.unpack(HDR_FMT_RP, data))
            # it appears that we may have hit a truncation point if all of the
            # fields in the header are zeros
            if all(map(lambda v: v == 0, header)):
                return
            records_size = header.batch_size - HEADER_SIZE
            data = f.read(records_size)
            assert len(data) == records_size
            return Batch(index, header, data)
        assert len(data) == 0

    def __iter__(self):
        return RecordIter(self.header.record_count, self.records)


class BatchIterator:
    def __init__(self, path):
        self.file = open(path, "rb")
        self.idx = 0

    def __next__(self):
        b = Batch.from_stream(self.file, self.idx)
        if not b:
            raise StopIteration()
        self.idx += 1
        return b

    def __del__(self):
        self.file.close()


class Segment:
    def __init__(self, path):
        self.path = path

    def __iter__(self):
        return BatchIterator(self.path)


class Ntp:
    def __init__(self, base_dir, namespace, topic, partition, ntp_id):
        self.base_dir = base_dir
        self.nspace = namespace
        self.topic = topic
        self.partition = partition
        self.ntp_id = ntp_id
        self.path = os.path.join(self.base_dir, self.nspace, self.topic,
                                 f"{self.partition}_{self.ntp_id}")
        pattern = os.path.join(self.path, "*.log")
        self.segments = glob.iglob(pattern)

        def _base_offset(segment_path):
            m = SEGMENT_NAME_PATTERN.match(os.path.basename(segment_path))
            return int(m['base_offset'])

        self.segments = sorted(self.segments, key=_base_offset)

    def __str__(self):
        return "{0.nspace}/{0.topic}/{0.partition}_{0.ntp_id}".format(self)


class Store:
    def __init__(self, base_dir):
        self.base_dir = os.path.abspath(base_dir)
        self.ntps = []
        self.__search()

    def __search(self):
        dirs = os.walk(self.base_dir)
        for ntpd in (p[0] for p in dirs if not p[1]):
            if 'cloud_storage_cache' in ntpd:
                continue
            head, part_ntp_id = os.path.split(ntpd)
            [part, ntp_id] = part_ntp_id.split("_")
            head, topic = os.path.split(head)
            head, nspace = os.path.split(head)
            assert head == self.base_dir
            ntp = Ntp(self.base_dir, nspace, topic, int(part), int(ntp_id))
            self.ntps.append(ntp)
