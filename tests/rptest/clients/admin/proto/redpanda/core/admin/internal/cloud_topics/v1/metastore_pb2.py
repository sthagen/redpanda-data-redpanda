"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/cloud_topics/v1/metastore.proto')
_sym_db = _symbol_database.Default()
from ........proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ........proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
from ........proto.redpanda.core.common.v1 import ntp_pb2 as proto_dot_redpanda_dot_core_dot_common_dot_v1_dot_ntp__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\nBproto/redpanda/core/admin/internal/cloud_topics/v1/metastore.proto\x12,redpanda.core.admin.internal.cloud_topics.v1\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a\'proto/redpanda/core/common/v1/ntp.proto"O\n\x11GetOffsetsRequest\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition"\\\n\x12GetOffsetsResponse\x12F\n\x07offsets\x18\x02 \x01(\x0b25.redpanda.core.admin.internal.cloud_topics.v1.Offsets"4\n\x07Offsets\x12\x14\n\x0cstart_offset\x18\x01 \x01(\x03\x12\x13\n\x0bnext_offset\x18\x02 \x01(\x032\xac\x01\n\x10MetastoreService\x12\x97\x01\n\nGetOffsets\x12?.redpanda.core.admin.internal.cloud_topics.v1.GetOffsetsRequest\x1a@.redpanda.core.admin.internal.cloud_topics.v1.GetOffsetsResponse"\x06\xea\x92\x19\x02\x10\x03B\x1b\xea\x92\x19\x17proto::admin::metastoreb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x17proto::admin::metastore'
    _globals['_METASTORESERVICE'].methods_by_name['GetOffsets']._loaded_options = None
    _globals['_METASTORESERVICE'].methods_by_name['GetOffsets']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_GETOFFSETSREQUEST']._serialized_start = 235
    _globals['_GETOFFSETSREQUEST']._serialized_end = 314
    _globals['_GETOFFSETSRESPONSE']._serialized_start = 316
    _globals['_GETOFFSETSRESPONSE']._serialized_end = 408
    _globals['_OFFSETS']._serialized_start = 410
    _globals['_OFFSETS']._serialized_end = 462
    _globals['_METASTORESERVICE']._serialized_start = 465
    _globals['_METASTORESERVICE']._serialized_end = 637