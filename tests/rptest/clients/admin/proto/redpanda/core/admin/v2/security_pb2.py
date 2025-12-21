"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/security.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n+proto/redpanda/core/admin/v2/security.proto\x12\x16redpanda.core.admin.v2\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a\x1fgoogle/protobuf/timestamp.proto"\x1c\n\x1aResolveOidcIdentityRequest"\\\n\x1bResolveOidcIdentityResponse\x12\x11\n\tprincipal\x18\x01 \x01(\t\x12*\n\x06expire\x18\x02 \x01(\x0b2\x1a.google.protobuf.Timestamp"\x18\n\x16RefreshOidcKeysRequest"\x19\n\x17RefreshOidcKeysResponse"\x1b\n\x19RevokeOidcSessionsRequest"\x1c\n\x1aRevokeOidcSessionsResponse2\x9c\x03\n\x0fSecurityService\x12\x86\x01\n\x13ResolveOidcIdentity\x122.redpanda.core.admin.v2.ResolveOidcIdentityRequest\x1a3.redpanda.core.admin.v2.ResolveOidcIdentityResponse"\x06\xea\x92\x19\x02\x10\x02\x12z\n\x0fRefreshOidcKeys\x12..redpanda.core.admin.v2.RefreshOidcKeysRequest\x1a/.redpanda.core.admin.v2.RefreshOidcKeysResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x83\x01\n\x12RevokeOidcSessions\x121.redpanda.core.admin.v2.RevokeOidcSessionsRequest\x1a2.redpanda.core.admin.v2.RevokeOidcSessionsResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.security_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_SECURITYSERVICE'].methods_by_name['ResolveOidcIdentity']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['ResolveOidcIdentity']._serialized_options = b'\xea\x92\x19\x02\x10\x02'
    _globals['_SECURITYSERVICE'].methods_by_name['RefreshOidcKeys']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['RefreshOidcKeys']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SECURITYSERVICE'].methods_by_name['RevokeOidcSessions']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['RevokeOidcSessions']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_RESOLVEOIDCIDENTITYREQUEST']._serialized_start = 182
    _globals['_RESOLVEOIDCIDENTITYREQUEST']._serialized_end = 210
    _globals['_RESOLVEOIDCIDENTITYRESPONSE']._serialized_start = 212
    _globals['_RESOLVEOIDCIDENTITYRESPONSE']._serialized_end = 304
    _globals['_REFRESHOIDCKEYSREQUEST']._serialized_start = 306
    _globals['_REFRESHOIDCKEYSREQUEST']._serialized_end = 330
    _globals['_REFRESHOIDCKEYSRESPONSE']._serialized_start = 332
    _globals['_REFRESHOIDCKEYSRESPONSE']._serialized_end = 357
    _globals['_REVOKEOIDCSESSIONSREQUEST']._serialized_start = 359
    _globals['_REVOKEOIDCSESSIONSREQUEST']._serialized_end = 386
    _globals['_REVOKEOIDCSESSIONSRESPONSE']._serialized_start = 388
    _globals['_REVOKEOIDCSESSIONSRESPONSE']._serialized_end = 416
    _globals['_SECURITYSERVICE']._serialized_start = 419
    _globals['_SECURITYSERVICE']._serialized_end = 831