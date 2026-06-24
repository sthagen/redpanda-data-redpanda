"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/iceberg.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n*proto/redpanda/core/admin/v2/iceberg.proto\x12\x16redpanda.core.admin.v2\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"0\n\x17GetIcebergStatusRequest\x12\x15\n\rtopics_filter\x18\x01 \x03(\t"\x8e\x01\n\x18GetIcebergStatusResponse\x126\n\x07catalog\x18\x01 \x01(\x0b2%.redpanda.core.admin.v2.CatalogHealth\x12:\n\x06topics\x18\x02 \x03(\x0b2*.redpanda.core.admin.v2.TopicIcebergStatus"M\n\rCatalogHealth\x12\x11\n\treachable\x18\x01 \x01(\x08\x12\x12\n\nerror_code\x18\x02 \x01(\t\x12\x15\n\rerror_message\x18\x03 \x01(\t"\xaf\x01\n\x12TopicIcebergStatus\x12\r\n\x05topic\x18\x01 \x01(\t\x12F\n\x0flifecycle_state\x18\x02 \x01(\x0e2-.redpanda.core.admin.v2.IcebergLifecycleState\x12B\n\npartitions\x18\x03 \x03(\x0b2..redpanda.core.admin.v2.PartitionIcebergStatus"\xbd\x01\n\x16PartitionIcebergStatus\x12\x11\n\tpartition\x18\x01 \x01(\x05\x12#\n\x16last_translated_offset\x18\x02 \x01(\x03H\x00\x88\x01\x01\x12"\n\x15last_committed_offset\x18\x03 \x01(\x03H\x01\x88\x01\x01\x12\x12\n\ncommit_lag\x18\x04 \x01(\x03B\x19\n\x17_last_translated_offsetB\x18\n\x16_last_committed_offset*\xaa\x01\n\x15IcebergLifecycleState\x12\'\n#ICEBERG_LIFECYCLE_STATE_UNSPECIFIED\x10\x00\x12 \n\x1cICEBERG_LIFECYCLE_STATE_LIVE\x10\x01\x12"\n\x1eICEBERG_LIFECYCLE_STATE_CLOSED\x10\x02\x12"\n\x1eICEBERG_LIFECYCLE_STATE_PURGED\x10\x032\x8f\x01\n\x0eIcebergService\x12}\n\x10GetIcebergStatus\x12/.redpanda.core.admin.v2.GetIcebergStatusRequest\x1a0.redpanda.core.admin.v2.GetIcebergStatusResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.iceberg_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_ICEBERGSERVICE'].methods_by_name['GetIcebergStatus']._loaded_options = None
    _globals['_ICEBERGSERVICE'].methods_by_name['GetIcebergStatus']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_ICEBERGLIFECYCLESTATE']._serialized_start = 793
    _globals['_ICEBERGLIFECYCLESTATE']._serialized_end = 963
    _globals['_GETICEBERGSTATUSREQUEST']._serialized_start = 148
    _globals['_GETICEBERGSTATUSREQUEST']._serialized_end = 196
    _globals['_GETICEBERGSTATUSRESPONSE']._serialized_start = 199
    _globals['_GETICEBERGSTATUSRESPONSE']._serialized_end = 341
    _globals['_CATALOGHEALTH']._serialized_start = 343
    _globals['_CATALOGHEALTH']._serialized_end = 420
    _globals['_TOPICICEBERGSTATUS']._serialized_start = 423
    _globals['_TOPICICEBERGSTATUS']._serialized_end = 598
    _globals['_PARTITIONICEBERGSTATUS']._serialized_start = 601
    _globals['_PARTITIONICEBERGSTATUS']._serialized_end = 790
    _globals['_ICEBERGSERVICE']._serialized_start = 966
    _globals['_ICEBERGSERVICE']._serialized_end = 1109