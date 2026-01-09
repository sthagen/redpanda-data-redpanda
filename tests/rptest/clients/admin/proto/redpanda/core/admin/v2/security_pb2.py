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
from ......google.api import field_behavior_pb2 as google_dot_api_dot_field__behavior__pb2
from ......google.api import resource_pb2 as google_dot_api_dot_resource__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n+proto/redpanda/core/admin/v2/security.proto\x12\x16redpanda.core.admin.v2\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a\x1fgoogle/api/field_behavior.proto\x1a\x19google/api/resource.proto\x1a\x1fgoogle/protobuf/timestamp.proto"\x8e\x01\n\x04Role\x12\x14\n\x04name\x18\x01 \x01(\tB\x06\xe0A\x02\xe0A\x05\x123\n\x07members\x18\x02 \x03(\x0b2".redpanda.core.admin.v2.RoleMember:;\xeaA8\n(redpanda.core.admin.SecurityService/Role\x12\x0croles/{role}"D\n\x11CreateRoleRequest\x12/\n\x04role\x18\x01 \x01(\x0b2\x1c.redpanda.core.admin.v2.RoleB\x03\xe0A\x02"@\n\x12CreateRoleResponse\x12*\n\x04role\x18\x01 \x01(\x0b2\x1c.redpanda.core.admin.v2.Role"P\n\x0eGetRoleRequest\x12>\n\x04name\x18\x01 \x01(\tB0\xe0A\x02\xfaA*\n(redpanda.core.admin.SecurityService/Role"=\n\x0fGetRoleResponse\x12*\n\x04role\x18\x01 \x01(\x0b2\x1c.redpanda.core.admin.v2.Role"\x12\n\x10ListRolesRequest"@\n\x11ListRolesResponse\x12+\n\x05roles\x18\x01 \x03(\x0b2\x1c.redpanda.core.admin.v2.Role"\x96\x01\n\x15AddRoleMembersRequest\x12C\n\trole_name\x18\x01 \x01(\tB0\xe0A\x02\xfaA*\n(redpanda.core.admin.SecurityService/Role\x128\n\x07members\x18\x02 \x03(\x0b2".redpanda.core.admin.v2.RoleMemberB\x03\xe0A\x02"D\n\x16AddRoleMembersResponse\x12*\n\x04role\x18\x01 \x01(\x0b2\x1c.redpanda.core.admin.v2.Role"\x99\x01\n\x18RemoveRoleMembersRequest\x12C\n\trole_name\x18\x01 \x01(\tB0\xe0A\x02\xfaA*\n(redpanda.core.admin.SecurityService/Role\x128\n\x07members\x18\x02 \x03(\x0b2".redpanda.core.admin.v2.RoleMemberB\x03\xe0A\x02"G\n\x19RemoveRoleMembersResponse\x12*\n\x04role\x18\x01 \x01(\x0b2\x1c.redpanda.core.admin.v2.Role"h\n\x11DeleteRoleRequest\x12>\n\x04name\x18\x01 \x01(\tB0\xe0A\x02\xfaA*\n(redpanda.core.admin.SecurityService/Role\x12\x13\n\x0bdelete_acls\x18\x02 \x01(\x08"\x14\n\x12DeleteRoleResponse"\x1d\n\x1bListCurrentUserRolesRequest"_\n\x1cListCurrentUserRolesResponse\x12?\n\x05roles\x18\x01 \x03(\tB0\xe0A\x03\xfaA*\n(redpanda.core.admin.SecurityService/Role"\x1c\n\x1aResolveOidcIdentityRequest"l\n\x1bResolveOidcIdentityResponse\x12\x11\n\tprincipal\x18\x01 \x01(\t\x12*\n\x06expire\x18\x02 \x01(\x0b2\x1a.google.protobuf.Timestamp\x12\x0e\n\x06groups\x18\x03 \x03(\t"\x18\n\x16RefreshOidcKeysRequest"\x19\n\x17RefreshOidcKeysResponse"\x1b\n\x19RevokeOidcSessionsRequest"\x1c\n\x1aRevokeOidcSessionsResponse"\x18\n\x08RoleUser\x12\x0c\n\x04name\x18\x01 \x01(\t"H\n\nRoleMember\x120\n\x04user\x18\x01 \x01(\x0b2 .redpanda.core.admin.v2.RoleUserH\x00B\x08\n\x06member2\xcc\t\n\x0fSecurityService\x12k\n\nCreateRole\x12).redpanda.core.admin.v2.CreateRoleRequest\x1a*.redpanda.core.admin.v2.CreateRoleResponse"\x06\xea\x92\x19\x02\x10\x03\x12b\n\x07GetRole\x12&.redpanda.core.admin.v2.GetRoleRequest\x1a\'.redpanda.core.admin.v2.GetRoleResponse"\x06\xea\x92\x19\x02\x10\x03\x12h\n\tListRoles\x12(.redpanda.core.admin.v2.ListRolesRequest\x1a).redpanda.core.admin.v2.ListRolesResponse"\x06\xea\x92\x19\x02\x10\x03\x12w\n\x0eAddRoleMembers\x12-.redpanda.core.admin.v2.AddRoleMembersRequest\x1a..redpanda.core.admin.v2.AddRoleMembersResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x80\x01\n\x11RemoveRoleMembers\x120.redpanda.core.admin.v2.RemoveRoleMembersRequest\x1a1.redpanda.core.admin.v2.RemoveRoleMembersResponse"\x06\xea\x92\x19\x02\x10\x03\x12k\n\nDeleteRole\x12).redpanda.core.admin.v2.DeleteRoleRequest\x1a*.redpanda.core.admin.v2.DeleteRoleResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x89\x01\n\x14ListCurrentUserRoles\x123.redpanda.core.admin.v2.ListCurrentUserRolesRequest\x1a4.redpanda.core.admin.v2.ListCurrentUserRolesResponse"\x06\xea\x92\x19\x02\x10\x02\x12\x86\x01\n\x13ResolveOidcIdentity\x122.redpanda.core.admin.v2.ResolveOidcIdentityRequest\x1a3.redpanda.core.admin.v2.ResolveOidcIdentityResponse"\x06\xea\x92\x19\x02\x10\x02\x12z\n\x0fRefreshOidcKeys\x12..redpanda.core.admin.v2.RefreshOidcKeysRequest\x1a/.redpanda.core.admin.v2.RefreshOidcKeysResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x83\x01\n\x12RevokeOidcSessions\x121.redpanda.core.admin.v2.RevokeOidcSessionsRequest\x1a2.redpanda.core.admin.v2.RevokeOidcSessionsResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.security_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_ROLE'].fields_by_name['name']._loaded_options = None
    _globals['_ROLE'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xe0A\x05'
    _globals['_ROLE']._loaded_options = None
    _globals['_ROLE']._serialized_options = b'\xeaA8\n(redpanda.core.admin.SecurityService/Role\x12\x0croles/{role}'
    _globals['_CREATEROLEREQUEST'].fields_by_name['role']._loaded_options = None
    _globals['_CREATEROLEREQUEST'].fields_by_name['role']._serialized_options = b'\xe0A\x02'
    _globals['_GETROLEREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_GETROLEREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA*\n(redpanda.core.admin.SecurityService/Role'
    _globals['_ADDROLEMEMBERSREQUEST'].fields_by_name['role_name']._loaded_options = None
    _globals['_ADDROLEMEMBERSREQUEST'].fields_by_name['role_name']._serialized_options = b'\xe0A\x02\xfaA*\n(redpanda.core.admin.SecurityService/Role'
    _globals['_ADDROLEMEMBERSREQUEST'].fields_by_name['members']._loaded_options = None
    _globals['_ADDROLEMEMBERSREQUEST'].fields_by_name['members']._serialized_options = b'\xe0A\x02'
    _globals['_REMOVEROLEMEMBERSREQUEST'].fields_by_name['role_name']._loaded_options = None
    _globals['_REMOVEROLEMEMBERSREQUEST'].fields_by_name['role_name']._serialized_options = b'\xe0A\x02\xfaA*\n(redpanda.core.admin.SecurityService/Role'
    _globals['_REMOVEROLEMEMBERSREQUEST'].fields_by_name['members']._loaded_options = None
    _globals['_REMOVEROLEMEMBERSREQUEST'].fields_by_name['members']._serialized_options = b'\xe0A\x02'
    _globals['_DELETEROLEREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_DELETEROLEREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA*\n(redpanda.core.admin.SecurityService/Role'
    _globals['_LISTCURRENTUSERROLESRESPONSE'].fields_by_name['roles']._loaded_options = None
    _globals['_LISTCURRENTUSERROLESRESPONSE'].fields_by_name['roles']._serialized_options = b'\xe0A\x03\xfaA*\n(redpanda.core.admin.SecurityService/Role'
    _globals['_SECURITYSERVICE'].methods_by_name['CreateRole']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['CreateRole']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SECURITYSERVICE'].methods_by_name['GetRole']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['GetRole']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SECURITYSERVICE'].methods_by_name['ListRoles']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['ListRoles']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SECURITYSERVICE'].methods_by_name['AddRoleMembers']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['AddRoleMembers']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SECURITYSERVICE'].methods_by_name['RemoveRoleMembers']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['RemoveRoleMembers']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SECURITYSERVICE'].methods_by_name['DeleteRole']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['DeleteRole']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SECURITYSERVICE'].methods_by_name['ListCurrentUserRoles']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['ListCurrentUserRoles']._serialized_options = b'\xea\x92\x19\x02\x10\x02'
    _globals['_SECURITYSERVICE'].methods_by_name['ResolveOidcIdentity']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['ResolveOidcIdentity']._serialized_options = b'\xea\x92\x19\x02\x10\x02'
    _globals['_SECURITYSERVICE'].methods_by_name['RefreshOidcKeys']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['RefreshOidcKeys']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SECURITYSERVICE'].methods_by_name['RevokeOidcSessions']._loaded_options = None
    _globals['_SECURITYSERVICE'].methods_by_name['RevokeOidcSessions']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_ROLE']._serialized_start = 243
    _globals['_ROLE']._serialized_end = 385
    _globals['_CREATEROLEREQUEST']._serialized_start = 387
    _globals['_CREATEROLEREQUEST']._serialized_end = 455
    _globals['_CREATEROLERESPONSE']._serialized_start = 457
    _globals['_CREATEROLERESPONSE']._serialized_end = 521
    _globals['_GETROLEREQUEST']._serialized_start = 523
    _globals['_GETROLEREQUEST']._serialized_end = 603
    _globals['_GETROLERESPONSE']._serialized_start = 605
    _globals['_GETROLERESPONSE']._serialized_end = 666
    _globals['_LISTROLESREQUEST']._serialized_start = 668
    _globals['_LISTROLESREQUEST']._serialized_end = 686
    _globals['_LISTROLESRESPONSE']._serialized_start = 688
    _globals['_LISTROLESRESPONSE']._serialized_end = 752
    _globals['_ADDROLEMEMBERSREQUEST']._serialized_start = 755
    _globals['_ADDROLEMEMBERSREQUEST']._serialized_end = 905
    _globals['_ADDROLEMEMBERSRESPONSE']._serialized_start = 907
    _globals['_ADDROLEMEMBERSRESPONSE']._serialized_end = 975
    _globals['_REMOVEROLEMEMBERSREQUEST']._serialized_start = 978
    _globals['_REMOVEROLEMEMBERSREQUEST']._serialized_end = 1131
    _globals['_REMOVEROLEMEMBERSRESPONSE']._serialized_start = 1133
    _globals['_REMOVEROLEMEMBERSRESPONSE']._serialized_end = 1204
    _globals['_DELETEROLEREQUEST']._serialized_start = 1206
    _globals['_DELETEROLEREQUEST']._serialized_end = 1310
    _globals['_DELETEROLERESPONSE']._serialized_start = 1312
    _globals['_DELETEROLERESPONSE']._serialized_end = 1332
    _globals['_LISTCURRENTUSERROLESREQUEST']._serialized_start = 1334
    _globals['_LISTCURRENTUSERROLESREQUEST']._serialized_end = 1363
    _globals['_LISTCURRENTUSERROLESRESPONSE']._serialized_start = 1365
    _globals['_LISTCURRENTUSERROLESRESPONSE']._serialized_end = 1460
    _globals['_RESOLVEOIDCIDENTITYREQUEST']._serialized_start = 1462
    _globals['_RESOLVEOIDCIDENTITYREQUEST']._serialized_end = 1490
    _globals['_RESOLVEOIDCIDENTITYRESPONSE']._serialized_start = 1492
    _globals['_RESOLVEOIDCIDENTITYRESPONSE']._serialized_end = 1600
    _globals['_REFRESHOIDCKEYSREQUEST']._serialized_start = 1602
    _globals['_REFRESHOIDCKEYSREQUEST']._serialized_end = 1626
    _globals['_REFRESHOIDCKEYSRESPONSE']._serialized_start = 1628
    _globals['_REFRESHOIDCKEYSRESPONSE']._serialized_end = 1653
    _globals['_REVOKEOIDCSESSIONSREQUEST']._serialized_start = 1655
    _globals['_REVOKEOIDCSESSIONSREQUEST']._serialized_end = 1682
    _globals['_REVOKEOIDCSESSIONSRESPONSE']._serialized_start = 1684
    _globals['_REVOKEOIDCSESSIONSRESPONSE']._serialized_end = 1712
    _globals['_ROLEUSER']._serialized_start = 1714
    _globals['_ROLEUSER']._serialized_end = 1738
    _globals['_ROLEMEMBER']._serialized_start = 1740
    _globals['_ROLEMEMBER']._serialized_end = 1812
    _globals['_SECURITYSERVICE']._serialized_start = 1815
    _globals['_SECURITYSERVICE']._serialized_end = 3043