"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/v1/breakglass.proto')
_sym_db = _symbol_database.Default()
from .......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from .......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n6proto/redpanda/core/admin/internal/v1/breakglass.proto\x12\x1fredpanda.core.admin.internal.v1\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"]\n&ControllerForcedReconfigurationRequest\x12\x15\n\rdead_node_ids\x18\x01 \x03(\x05\x12\x1c\n\x14surviving_node_count\x18\x02 \x01(\r")\n\'ControllerForcedReconfigurationResponse2\xd2\x01\n\x11BreakglassService\x12\xbc\x01\n\x1fControllerForcedReconfiguration\x12G.redpanda.core.admin.internal.v1.ControllerForcedReconfigurationRequest\x1aH.redpanda.core.admin.internal.v1.ControllerForcedReconfigurationResponse"\x06\xea\x92\x19\x02\x10\x03B\x1a\xea\x92\x19\x16proto::admin::internalb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.v1.breakglass_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x16proto::admin::internal'
    _globals['_BREAKGLASSSERVICE'].methods_by_name['ControllerForcedReconfiguration']._loaded_options = None
    _globals['_BREAKGLASSSERVICE'].methods_by_name['ControllerForcedReconfiguration']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_CONTROLLERFORCEDRECONFIGURATIONREQUEST']._serialized_start = 169
    _globals['_CONTROLLERFORCEDRECONFIGURATIONREQUEST']._serialized_end = 262
    _globals['_CONTROLLERFORCEDRECONFIGURATIONRESPONSE']._serialized_start = 264
    _globals['_CONTROLLERFORCEDRECONFIGURATIONRESPONSE']._serialized_end = 305
    _globals['_BREAKGLASSSERVICE']._serialized_start = 308
    _globals['_BREAKGLASSSERVICE']._serialized_end = 518