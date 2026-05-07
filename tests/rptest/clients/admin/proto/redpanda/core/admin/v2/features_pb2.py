"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/features.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n+proto/redpanda/core/admin/v2/features.proto\x12\x16redpanda.core.admin.v2\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"\x18\n\x16FinalizeUpgradeRequest"\x19\n\x17FinalizeUpgradeResponse2\x8d\x01\n\x0fFeaturesService\x12z\n\x0fFinalizeUpgrade\x12..redpanda.core.admin.v2.FinalizeUpgradeRequest\x1a/.redpanda.core.admin.v2.FinalizeUpgradeResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.features_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_FEATURESSERVICE'].methods_by_name['FinalizeUpgrade']._loaded_options = None
    _globals['_FEATURESSERVICE'].methods_by_name['FinalizeUpgrade']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_FINALIZEUPGRADEREQUEST']._serialized_start = 149
    _globals['_FINALIZEUPGRADEREQUEST']._serialized_end = 173
    _globals['_FINALIZEUPGRADERESPONSE']._serialized_start = 175
    _globals['_FINALIZEUPGRADERESPONSE']._serialized_end = 200
    _globals['_FEATURESSERVICE']._serialized_start = 203
    _globals['_FEATURESSERVICE']._serialized_end = 344