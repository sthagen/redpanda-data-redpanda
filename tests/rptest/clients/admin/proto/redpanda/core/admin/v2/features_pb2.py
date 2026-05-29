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
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n+proto/redpanda/core/admin/v2/features.proto\x12\x16redpanda.core.admin.v2\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"\x18\n\x16FinalizeUpgradeRequest"\x19\n\x17FinalizeUpgradeResponse"\x19\n\x17GetUpgradeStatusRequest"x\n\rMemberVersion\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12\x17\n\x0flogical_version\x18\x02 \x01(\x03\x12\x15\n\rversion_known\x18\x03 \x01(\x08\x12\r\n\x05alive\x18\x04 \x01(\x08\x12\x17\n\x0frelease_version\x18\x05 \x01(\t"\xeb\x01\n\x18GetUpgradeStatusResponse\x128\n\x05state\x18\x01 \x01(\x0e2).redpanda.core.admin.v2.FinalizationState\x12\x16\n\x0eactive_version\x18\x02 \x01(\x03\x12"\n\x1aversion_after_finalization\x18\x03 \x01(\x03\x12!\n\x19auto_finalization_enabled\x18\x04 \x01(\x08\x126\n\x07members\x18\x05 \x03(\x0b2%.redpanda.core.admin.v2.MemberVersion*\xaf\x01\n\x11FinalizationState\x12"\n\x1eFINALIZATION_STATE_UNSPECIFIED\x10\x00\x12 \n\x1cFINALIZATION_STATE_FINALIZED\x10\x01\x12(\n$FINALIZATION_STATE_READY_TO_FINALIZE\x10\x02\x12*\n&FINALIZATION_STATE_UPGRADE_IN_PROGRESS\x10\x032\x8c\x02\n\x0fFeaturesService\x12z\n\x0fFinalizeUpgrade\x12..redpanda.core.admin.v2.FinalizeUpgradeRequest\x1a/.redpanda.core.admin.v2.FinalizeUpgradeResponse"\x06\xea\x92\x19\x02\x10\x03\x12}\n\x10GetUpgradeStatus\x12/.redpanda.core.admin.v2.GetUpgradeStatusRequest\x1a0.redpanda.core.admin.v2.GetUpgradeStatusResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.features_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_FEATURESSERVICE'].methods_by_name['FinalizeUpgrade']._loaded_options = None
    _globals['_FEATURESSERVICE'].methods_by_name['FinalizeUpgrade']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_FEATURESSERVICE'].methods_by_name['GetUpgradeStatus']._loaded_options = None
    _globals['_FEATURESSERVICE'].methods_by_name['GetUpgradeStatus']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_FINALIZATIONSTATE']._serialized_start = 590
    _globals['_FINALIZATIONSTATE']._serialized_end = 765
    _globals['_FINALIZEUPGRADEREQUEST']._serialized_start = 149
    _globals['_FINALIZEUPGRADEREQUEST']._serialized_end = 173
    _globals['_FINALIZEUPGRADERESPONSE']._serialized_start = 175
    _globals['_FINALIZEUPGRADERESPONSE']._serialized_end = 200
    _globals['_GETUPGRADESTATUSREQUEST']._serialized_start = 202
    _globals['_GETUPGRADESTATUSREQUEST']._serialized_end = 227
    _globals['_MEMBERVERSION']._serialized_start = 229
    _globals['_MEMBERVERSION']._serialized_end = 349
    _globals['_GETUPGRADESTATUSRESPONSE']._serialized_start = 352
    _globals['_GETUPGRADESTATUSRESPONSE']._serialized_end = 587
    _globals['_FEATURESSERVICE']._serialized_start = 768
    _globals['_FEATURESSERVICE']._serialized_end = 1036