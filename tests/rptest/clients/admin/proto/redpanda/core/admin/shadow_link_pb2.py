"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/shadow_link.proto')
_sym_db = _symbol_database.Default()
from .....proto.redpanda.pbgen import options_pb2 as proto_dot_redpanda_dot_pbgen_dot_options__pb2
from .....proto.redpanda.pbgen import rpc_pb2 as proto_dot_redpanda_dot_pbgen_dot_rpc__pb2
from .....proto.redpanda.core.common import acl_pb2 as proto_dot_redpanda_dot_core_dot_common_dot_acl__pb2
from .....google.api import field_behavior_pb2 as google_dot_api_dot_field__behavior__pb2
from .....google.api import field_info_pb2 as google_dot_api_dot_field__info__pb2
from .....google.api import resource_pb2 as google_dot_api_dot_resource__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import field_mask_pb2 as google_dot_protobuf_dot_field__mask__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n+proto/redpanda/core/admin/shadow_link.proto\x12\x13redpanda.core.admin\x1a"proto/redpanda/pbgen/options.proto\x1a\x1eproto/redpanda/pbgen/rpc.proto\x1a$proto/redpanda/core/common/acl.proto\x1a\x1fgoogle/api/field_behavior.proto\x1a\x1bgoogle/api/field_info.proto\x1a\x19google/api/resource.proto\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a google/protobuf/field_mask.proto"\xbc\x01\n\nShadowLink\x12\x11\n\x04name\x18\x01 \x01(\tB\x03\xe0A\x02\x12\x18\n\x03uid\x18\x02 \x01(\tB\x0b\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01\x12E\n\x0econfigurations\x18\x03 \x01(\x0b2-.redpanda.core.admin.ShadowLinkConfigurations\x12:\n\x06status\x18\x04 \x01(\x0b2%.redpanda.core.admin.ShadowLinkStatusB\x03\xe0A\x03"O\n\x17CreateShadowLinkRequest\x124\n\x0bshadow_link\x18\x01 \x01(\x0b2\x1f.redpanda.core.admin.ShadowLink"P\n\x18CreateShadowLinkResponse\x124\n\x0bshadow_link\x18\x01 \x01(\x0b2\x1f.redpanda.core.admin.ShadowLink"a\n\x17DeleteShadowLinkRequest\x12F\n\x04name\x18\x01 \x01(\tB8\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink"\x1a\n\x18DeleteShadowLinkResponse"^\n\x14GetShadowLinkRequest\x12F\n\x04name\x18\x01 \x01(\tB8\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink"M\n\x15GetShadowLinkResponse\x124\n\x0bshadow_link\x18\x01 \x01(\x0b2\x1f.redpanda.core.admin.ShadowLink"\x18\n\x16ListShadowLinksRequest"P\n\x17ListShadowLinksResponse\x125\n\x0cshadow_links\x18\x01 \x03(\x0b2\x1f.redpanda.core.admin.ShadowLink"\x80\x01\n\x17UpdateShadowLinkRequest\x124\n\x0bshadow_link\x18\x01 \x01(\x0b2\x1f.redpanda.core.admin.ShadowLink\x12/\n\x0bupdate_mask\x18\x02 \x01(\x0b2\x1a.google.protobuf.FieldMask"P\n\x18UpdateShadowLinkResponse\x124\n\x0bshadow_link\x18\x01 \x01(\x0b2\x1f.redpanda.core.admin.ShadowLink"y\n\x0fFailOverRequest\x12F\n\x04name\x18\x01 \x01(\tB8\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink\x12\x1e\n\x11shadow_topic_name\x18\x02 \x01(\tB\x03\xe0A\x01"H\n\x10FailOverResponse\x124\n\x0bshadow_link\x18\x01 \x01(\x0b2\x1f.redpanda.core.admin.ShadowLink"\xdb\x02\n\x18ShadowLinkConfigurations\x12D\n\x0eclient_options\x18\x01 \x01(\x0b2,.redpanda.core.admin.ShadowLinkClientOptions\x12R\n\x1btopic_metadata_sync_options\x18\x02 \x01(\x0b2-.redpanda.core.admin.TopicMetadataSyncOptions\x12T\n\x1cconsumer_offset_sync_options\x18\x03 \x01(\x0b2..redpanda.core.admin.ConsumerOffsetSyncOptions\x12O\n\x15security_sync_options\x18\x04 \x01(\x0b20.redpanda.core.admin.SecuritySettingsSyncOptions"\xd4\x03\n\x17ShadowLinkClientOptions\x12\x1e\n\x11bootstrap_servers\x18\x01 \x03(\tB\x03\xe0A\x02\x12\x16\n\tclient_id\x18\x02 \x01(\tB\x03\xe0A\x03\x12\x19\n\x11source_cluster_id\x18\x03 \x01(\t\x12;\n\x0ctls_settings\x18\x04 \x01(\x0b2 .redpanda.core.admin.TLSSettingsH\x00\x88\x01\x01\x12T\n\x1cauthentication_configuration\x18\x05 \x01(\x0b2).redpanda.core.admin.AuthenticationConfigH\x01\x88\x01\x01\x12\x1b\n\x13metadata_max_age_ms\x18\x06 \x01(\x05\x12\x1d\n\x15connection_timeout_ms\x18\x07 \x01(\x05\x12\x18\n\x10retry_backoff_ms\x18\x08 \x01(\x05\x12\x19\n\x11fetch_wait_max_ms\x18\t \x01(\x05\x12\x17\n\x0ffetch_min_bytes\x18\n \x01(\x05\x12\x17\n\x0ffetch_max_bytes\x18\x0b \x01(\x05B\x0f\n\r_tls_settingsB\x1f\n\x1d_authentication_configuration"\xa2\x01\n\x18TopicMetadataSyncOptions\x12+\n\x08interval\x18\x01 \x01(\x0b2\x19.google.protobuf.Duration\x126\n\rtopic_filters\x18\x02 \x03(\x0b2\x1f.redpanda.core.admin.NameFilter\x12!\n\x19shadowed_topic_properties\x18\x03 \x03(\t"\x91\x01\n\x19ConsumerOffsetSyncOptions\x12+\n\x08interval\x18\x01 \x01(\x0b2\x19.google.protobuf.Duration\x12\x0f\n\x07enabled\x18\x02 \x01(\x08\x126\n\rgroup_filters\x18\x03 \x03(\x0b2\x1f.redpanda.core.admin.NameFilter"\x84\x02\n\x1bSecuritySettingsSyncOptions\x12+\n\x08interval\x18\x01 \x01(\x0b2\x19.google.protobuf.Duration\x12\x0f\n\x07enabled\x18\x02 \x01(\x08\x125\n\x0crole_filters\x18\x03 \x03(\x0b2\x1f.redpanda.core.admin.NameFilter\x12;\n\x12scram_cred_filters\x18\x04 \x03(\x0b2\x1f.redpanda.core.admin.NameFilter\x123\n\x0bacl_filters\x18\x05 \x03(\x0b2\x1e.redpanda.core.admin.ACLFilter"\xa1\x01\n\x0bTLSSettings\x12A\n\x11tls_file_settings\x18\x01 \x01(\x0b2$.redpanda.core.admin.TLSFileSettingsH\x00\x12?\n\x10tls_pem_settings\x18\x02 \x01(\x0b2#.redpanda.core.admin.TLSPEMSettingsH\x00B\x0e\n\x0ctls_settings"i\n\x14AuthenticationConfig\x12?\n\x13scram_configuration\x18\x01 \x01(\x0b2 .redpanda.core.admin.ScramConfigH\x00B\x10\n\x0eauthentication"G\n\x0fTLSFileSettings\x12\x0f\n\x07ca_path\x18\x01 \x01(\t\x12\x10\n\x08key_path\x18\x02 \x01(\t\x12\x11\n\tcert_path\x18\x03 \x01(\t"Z\n\x0eTLSPEMSettings\x12\n\n\x02ca\x18\x01 \x01(\t\x12\x10\n\x03key\x18\x02 \x01(\tB\x03\xe0A\x04\x12\x1c\n\x0fkey_fingerprint\x18\x03 \x01(\tB\x03\xe0A\x03\x12\x0c\n\x04cert\x18\x04 \x01(\t"\xc9\x01\n\x0bScramConfig\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x15\n\x08password\x18\x02 \x01(\tB\x03\xe0A\x04\x12\x19\n\x0cpassword_set\x18\x03 \x01(\x08B\x03\xe0A\x03\x128\n\x0fpassword_set_at\x18\x04 \x01(\x0b2\x1a.google.protobuf.TimestampB\x03\xe0A\x03\x12<\n\x0fscram_mechanism\x18\x05 \x01(\x0e2#.redpanda.core.admin.ScramMechanism"\x88\x01\n\nNameFilter\x126\n\x0cpattern_type\x18\x01 \x01(\x0e2 .redpanda.core.admin.PatternType\x124\n\x0bfilter_type\x18\x02 \x01(\x0e2\x1f.redpanda.core.admin.FilterType\x12\x0c\n\x04name\x18\x03 \x01(\t"\x89\x01\n\tACLFilter\x12?\n\x0fresource_filter\x18\x01 \x01(\x0b2&.redpanda.core.admin.ACLResourceFilter\x12;\n\raccess_filter\x18\x02 \x01(\x0b2$.redpanda.core.admin.ACLAccessFilter"\x93\x01\n\x11ACLResourceFilter\x128\n\rresource_type\x18\x01 \x01(\x0e2!.redpanda.core.common.ACLResource\x126\n\x0cpattern_type\x18\x02 \x01(\x0e2 .redpanda.core.common.ACLPattern\x12\x0c\n\x04name\x18\x03 \x01(\t"\xab\x01\n\x0fACLAccessFilter\x12\x11\n\tprincipal\x18\x01 \x01(\t\x125\n\toperation\x18\x02 \x01(\x0e2".redpanda.core.common.ACLOperation\x12@\n\x0fpermission_type\x18\x03 \x01(\x0e2\'.redpanda.core.common.ACLPermissionType\x12\x0c\n\x04host\x18\x04 \x01(\t"\xd0\x01\n\x10ShadowLinkStatus\x123\n\x05state\x18\x01 \x01(\x0e2$.redpanda.core.admin.ShadowLinkState\x12@\n\rtask_statuses\x18\x02 \x03(\x0b2).redpanda.core.admin.ShadowLinkTaskStatus\x12E\n\x15shadow_topic_statuses\x18\x03 \x03(\x0b2&.redpanda.core.admin.ShadowTopicStatus"v\n\x14ShadowLinkTaskStatus\x12\x0c\n\x04name\x18\x01 \x01(\t\x12-\n\x05state\x18\x02 \x01(\x0e2\x1e.redpanda.core.admin.TaskState\x12\x0e\n\x06reason\x18\x03 \x01(\t\x12\x11\n\tbroker_id\x18\x04 \x01(\x05"\xb8\x01\n\x11ShadowTopicStatus\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x10\n\x08topic_id\x18\x02 \x01(\t\x124\n\x05state\x18\x03 \x01(\x0e2%.redpanda.core.admin.ShadowTopicState\x12M\n\x15partition_information\x18\x04 \x03(\x0b2..redpanda.core.admin.TopicPartitionInformation"\x8b\x01\n\x19TopicPartitionInformation\x12\x14\n\x0cpartition_id\x18\x01 \x01(\x03\x12!\n\x19source_last_stable_offset\x18\x02 \x01(\x03\x12\x1d\n\x15source_high_watermark\x18\x03 \x01(\x03\x12\x16\n\x0ehigh_watermark\x18\x04 \x01(\x03*\xb7\x01\n\x0fShadowLinkState\x12!\n\x1dSHADOW_LINK_STATE_UNSPECIFIED\x10\x00\x12\x1c\n\x18SHADOW_LINK_STATE_ACTIVE\x10\x01\x12\x1c\n\x18SHADOW_LINK_STATE_PAUSED\x10\x02\x12"\n\x1eSHADOW_LINK_STATE_FAILING_OVER\x10\x03\x12!\n\x1dSHADOW_LINK_STATE_FAILED_OVER\x10\x04*w\n\x0eScramMechanism\x12\x1f\n\x1bSCRAM_MECHANISM_UNSPECIFIED\x10\x00\x12!\n\x1dSCRAM_MECHANISM_SCRAM_SHA_256\x10\x01\x12!\n\x1dSCRAM_MECHANISM_SCRAM_SHA_512\x10\x02*^\n\x0bPatternType\x12\x1c\n\x18PATTERN_TYPE_UNSPECIFIED\x10\x00\x12\x18\n\x14PATTERN_TYPE_LITERAL\x10\x01\x12\x17\n\x13PATTERN_TYPE_PREFIX\x10\x02*[\n\nFilterType\x12\x1b\n\x17FILTER_TYPE_UNSPECIFIED\x10\x00\x12\x17\n\x13FILTER_TYPE_INCLUDE\x10\x01\x12\x17\n\x13FILTER_TYPE_EXCLUDE\x10\x02*\xaa\x01\n\tTaskState\x12\x1a\n\x16TASK_STATE_UNSPECIFIED\x10\x00\x12\x15\n\x11TASK_STATE_ACTIVE\x10\x01\x12\x15\n\x11TASK_STATE_PAUSED\x10\x02\x12\x1f\n\x1bTASK_STATE_LINK_UNAVAILABLE\x10\x03\x12\x1a\n\x16TASK_STATE_NOT_RUNNING\x10\x04\x12\x16\n\x12TASK_STATE_FAULTED\x10\x05*\x96\x01\n\x10ShadowTopicState\x12"\n\x1eSHADOW_TOPIC_STATE_UNSPECIFIED\x10\x00\x12\x1d\n\x19SHADOW_TOPIC_STATE_ACTIVE\x10\x01\x12\x1f\n\x1bSHADOW_TOPIC_STATE_PROMOTED\x10\x02\x12\x1e\n\x1aSHADOW_TOPIC_STATE_FAULTED\x10\x032\xc5\x05\n\x11ShadowLinkService\x12w\n\x10CreateShadowLink\x12,.redpanda.core.admin.CreateShadowLinkRequest\x1a-.redpanda.core.admin.CreateShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12w\n\x10DeleteShadowLink\x12,.redpanda.core.admin.DeleteShadowLinkRequest\x1a-.redpanda.core.admin.DeleteShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12n\n\rGetShadowLink\x12).redpanda.core.admin.GetShadowLinkRequest\x1a*.redpanda.core.admin.GetShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12t\n\x0fListShadowLinks\x12+.redpanda.core.admin.ListShadowLinksRequest\x1a,.redpanda.core.admin.ListShadowLinksResponse"\x06\xea\x92\x19\x02\x10\x03\x12w\n\x10UpdateShadowLink\x12,.redpanda.core.admin.UpdateShadowLinkRequest\x1a-.redpanda.core.admin.UpdateShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12_\n\x08FailOver\x12$.redpanda.core.admin.FailOverRequest\x1a%.redpanda.core.admin.FailOverResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.shadow_link_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_SHADOWLINK'].fields_by_name['name']._loaded_options = None
    _globals['_SHADOWLINK'].fields_by_name['name']._serialized_options = b'\xe0A\x02'
    _globals['_SHADOWLINK'].fields_by_name['uid']._loaded_options = None
    _globals['_SHADOWLINK'].fields_by_name['uid']._serialized_options = b'\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01'
    _globals['_SHADOWLINK'].fields_by_name['status']._loaded_options = None
    _globals['_SHADOWLINK'].fields_by_name['status']._serialized_options = b'\xe0A\x03'
    _globals['_DELETESHADOWLINKREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_DELETESHADOWLINKREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink'
    _globals['_GETSHADOWLINKREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_GETSHADOWLINKREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink'
    _globals['_FAILOVERREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_FAILOVERREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink'
    _globals['_FAILOVERREQUEST'].fields_by_name['shadow_topic_name']._loaded_options = None
    _globals['_FAILOVERREQUEST'].fields_by_name['shadow_topic_name']._serialized_options = b'\xe0A\x01'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['bootstrap_servers']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['bootstrap_servers']._serialized_options = b'\xe0A\x02'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['client_id']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['client_id']._serialized_options = b'\xe0A\x03'
    _globals['_TLSPEMSETTINGS'].fields_by_name['key']._loaded_options = None
    _globals['_TLSPEMSETTINGS'].fields_by_name['key']._serialized_options = b'\xe0A\x04'
    _globals['_TLSPEMSETTINGS'].fields_by_name['key_fingerprint']._loaded_options = None
    _globals['_TLSPEMSETTINGS'].fields_by_name['key_fingerprint']._serialized_options = b'\xe0A\x03'
    _globals['_SCRAMCONFIG'].fields_by_name['password']._loaded_options = None
    _globals['_SCRAMCONFIG'].fields_by_name['password']._serialized_options = b'\xe0A\x04'
    _globals['_SCRAMCONFIG'].fields_by_name['password_set']._loaded_options = None
    _globals['_SCRAMCONFIG'].fields_by_name['password_set']._serialized_options = b'\xe0A\x03'
    _globals['_SCRAMCONFIG'].fields_by_name['password_set_at']._loaded_options = None
    _globals['_SCRAMCONFIG'].fields_by_name['password_set_at']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['CreateShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['CreateShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['DeleteShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['DeleteShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['GetShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['GetShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['ListShadowLinks']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['ListShadowLinks']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['UpdateShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['UpdateShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['FailOver']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['FailOver']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSTATE']._serialized_start = 4837
    _globals['_SHADOWLINKSTATE']._serialized_end = 5020
    _globals['_SCRAMMECHANISM']._serialized_start = 5022
    _globals['_SCRAMMECHANISM']._serialized_end = 5141
    _globals['_PATTERNTYPE']._serialized_start = 5143
    _globals['_PATTERNTYPE']._serialized_end = 5237
    _globals['_FILTERTYPE']._serialized_start = 5239
    _globals['_FILTERTYPE']._serialized_end = 5330
    _globals['_TASKSTATE']._serialized_start = 5333
    _globals['_TASKSTATE']._serialized_end = 5503
    _globals['_SHADOWTOPICSTATE']._serialized_start = 5506
    _globals['_SHADOWTOPICSTATE']._serialized_end = 5656
    _globals['_SHADOWLINK']._serialized_start = 363
    _globals['_SHADOWLINK']._serialized_end = 551
    _globals['_CREATESHADOWLINKREQUEST']._serialized_start = 553
    _globals['_CREATESHADOWLINKREQUEST']._serialized_end = 632
    _globals['_CREATESHADOWLINKRESPONSE']._serialized_start = 634
    _globals['_CREATESHADOWLINKRESPONSE']._serialized_end = 714
    _globals['_DELETESHADOWLINKREQUEST']._serialized_start = 716
    _globals['_DELETESHADOWLINKREQUEST']._serialized_end = 813
    _globals['_DELETESHADOWLINKRESPONSE']._serialized_start = 815
    _globals['_DELETESHADOWLINKRESPONSE']._serialized_end = 841
    _globals['_GETSHADOWLINKREQUEST']._serialized_start = 843
    _globals['_GETSHADOWLINKREQUEST']._serialized_end = 937
    _globals['_GETSHADOWLINKRESPONSE']._serialized_start = 939
    _globals['_GETSHADOWLINKRESPONSE']._serialized_end = 1016
    _globals['_LISTSHADOWLINKSREQUEST']._serialized_start = 1018
    _globals['_LISTSHADOWLINKSREQUEST']._serialized_end = 1042
    _globals['_LISTSHADOWLINKSRESPONSE']._serialized_start = 1044
    _globals['_LISTSHADOWLINKSRESPONSE']._serialized_end = 1124
    _globals['_UPDATESHADOWLINKREQUEST']._serialized_start = 1127
    _globals['_UPDATESHADOWLINKREQUEST']._serialized_end = 1255
    _globals['_UPDATESHADOWLINKRESPONSE']._serialized_start = 1257
    _globals['_UPDATESHADOWLINKRESPONSE']._serialized_end = 1337
    _globals['_FAILOVERREQUEST']._serialized_start = 1339
    _globals['_FAILOVERREQUEST']._serialized_end = 1460
    _globals['_FAILOVERRESPONSE']._serialized_start = 1462
    _globals['_FAILOVERRESPONSE']._serialized_end = 1534
    _globals['_SHADOWLINKCONFIGURATIONS']._serialized_start = 1537
    _globals['_SHADOWLINKCONFIGURATIONS']._serialized_end = 1884
    _globals['_SHADOWLINKCLIENTOPTIONS']._serialized_start = 1887
    _globals['_SHADOWLINKCLIENTOPTIONS']._serialized_end = 2355
    _globals['_TOPICMETADATASYNCOPTIONS']._serialized_start = 2358
    _globals['_TOPICMETADATASYNCOPTIONS']._serialized_end = 2520
    _globals['_CONSUMEROFFSETSYNCOPTIONS']._serialized_start = 2523
    _globals['_CONSUMEROFFSETSYNCOPTIONS']._serialized_end = 2668
    _globals['_SECURITYSETTINGSSYNCOPTIONS']._serialized_start = 2671
    _globals['_SECURITYSETTINGSSYNCOPTIONS']._serialized_end = 2931
    _globals['_TLSSETTINGS']._serialized_start = 2934
    _globals['_TLSSETTINGS']._serialized_end = 3095
    _globals['_AUTHENTICATIONCONFIG']._serialized_start = 3097
    _globals['_AUTHENTICATIONCONFIG']._serialized_end = 3202
    _globals['_TLSFILESETTINGS']._serialized_start = 3204
    _globals['_TLSFILESETTINGS']._serialized_end = 3275
    _globals['_TLSPEMSETTINGS']._serialized_start = 3277
    _globals['_TLSPEMSETTINGS']._serialized_end = 3367
    _globals['_SCRAMCONFIG']._serialized_start = 3370
    _globals['_SCRAMCONFIG']._serialized_end = 3571
    _globals['_NAMEFILTER']._serialized_start = 3574
    _globals['_NAMEFILTER']._serialized_end = 3710
    _globals['_ACLFILTER']._serialized_start = 3713
    _globals['_ACLFILTER']._serialized_end = 3850
    _globals['_ACLRESOURCEFILTER']._serialized_start = 3853
    _globals['_ACLRESOURCEFILTER']._serialized_end = 4000
    _globals['_ACLACCESSFILTER']._serialized_start = 4003
    _globals['_ACLACCESSFILTER']._serialized_end = 4174
    _globals['_SHADOWLINKSTATUS']._serialized_start = 4177
    _globals['_SHADOWLINKSTATUS']._serialized_end = 4385
    _globals['_SHADOWLINKTASKSTATUS']._serialized_start = 4387
    _globals['_SHADOWLINKTASKSTATUS']._serialized_end = 4505
    _globals['_SHADOWTOPICSTATUS']._serialized_start = 4508
    _globals['_SHADOWTOPICSTATUS']._serialized_end = 4692
    _globals['_TOPICPARTITIONINFORMATION']._serialized_start = 4695
    _globals['_TOPICPARTITIONINFORMATION']._serialized_end = 4834
    _globals['_SHADOWLINKSERVICE']._serialized_start = 5659
    _globals['_SHADOWLINKSERVICE']._serialized_end = 6368