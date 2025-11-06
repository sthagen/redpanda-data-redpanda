"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/shadow_link_internal/v1/shadow_link_internal.proto')
_sym_db = _symbol_database.Default()
from ........proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ........proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
from ........proto.redpanda.core.admin.v2 import shadow_link_pb2 as proto_dot_redpanda_dot_core_dot_admin_dot_v2_dot_shadow__link__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\nUproto/redpanda/core/admin/internal/shadow_link_internal/v1/shadow_link_internal.proto\x12+redpanda.core.admin.internal.shadow_link.v1\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a.proto/redpanda/core/admin/v2/shadow_link.proto"O\n\x18RemoveShadowTopicRequest\x12\x18\n\x10shadow_link_name\x18\x01 \x01(\t\x12\x19\n\x11shadow_topic_name\x18\x02 \x01(\t"\x1b\n\x19RemoveShadowTopicResponse"\x96\x01\n"ForceUpdateShadowTopicStateRequest\x12\x18\n\x10shadow_link_name\x18\x01 \x01(\t\x12\x19\n\x11shadow_topic_name\x18\x02 \x01(\t\x12;\n\tnew_state\x18\x03 \x01(\x0e2(.redpanda.core.admin.v2.ShadowTopicState"%\n#ForceUpdateShadowTopicStateResponse2\x93\x03\n\x19ShadowLinkInternalService\x12\xaa\x01\n\x11RemoveShadowTopic\x12E.redpanda.core.admin.internal.shadow_link.v1.RemoveShadowTopicRequest\x1aF.redpanda.core.admin.internal.shadow_link.v1.RemoveShadowTopicResponse"\x06\xea\x92\x19\x02\x10\x03\x12\xc8\x01\n\x1bForceUpdateShadowTopicState\x12O.redpanda.core.admin.internal.shadow_link.v1.ForceUpdateShadowTopicStateRequest\x1aP.redpanda.core.admin.internal.shadow_link.v1.ForceUpdateShadowTopicStateResponse"\x06\xea\x92\x19\x02\x10\x03B\'\xea\x92\x19#proto::admin::internal::shadow_linkb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19#proto::admin::internal::shadow_link'
    _globals['_SHADOWLINKINTERNALSERVICE'].methods_by_name['RemoveShadowTopic']._loaded_options = None
    _globals['_SHADOWLINKINTERNALSERVICE'].methods_by_name['RemoveShadowTopic']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKINTERNALSERVICE'].methods_by_name['ForceUpdateShadowTopicState']._loaded_options = None
    _globals['_SHADOWLINKINTERNALSERVICE'].methods_by_name['ForceUpdateShadowTopicState']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_REMOVESHADOWTOPICREQUEST']._serialized_start = 260
    _globals['_REMOVESHADOWTOPICREQUEST']._serialized_end = 339
    _globals['_REMOVESHADOWTOPICRESPONSE']._serialized_start = 341
    _globals['_REMOVESHADOWTOPICRESPONSE']._serialized_end = 368
    _globals['_FORCEUPDATESHADOWTOPICSTATEREQUEST']._serialized_start = 371
    _globals['_FORCEUPDATESHADOWTOPICSTATEREQUEST']._serialized_end = 521
    _globals['_FORCEUPDATESHADOWTOPICSTATERESPONSE']._serialized_start = 523
    _globals['_FORCEUPDATESHADOWTOPICSTATERESPONSE']._serialized_end = 560
    _globals['_SHADOWLINKINTERNALSERVICE']._serialized_start = 563
    _globals['_SHADOWLINKINTERNALSERVICE']._serialized_end = 966