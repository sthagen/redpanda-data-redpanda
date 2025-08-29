"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/broker.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.pbgen import options_pb2 as proto_dot_redpanda_dot_pbgen_dot_options__pb2
from ......proto.redpanda.pbgen import rpc_pb2 as proto_dot_redpanda_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n)proto/redpanda/core/admin/v2/broker.proto\x12\x16redpanda.core.admin.v2\x1a"proto/redpanda/pbgen/options.proto\x1a\x1eproto/redpanda/pbgen/rpc.proto"#\n\x10GetBrokerRequest\x12\x0f\n\x07node_id\x18\x01 \x01(\x05"C\n\x11GetBrokerResponse\x12.\n\x06broker\x18\x01 \x01(\x0b2\x1e.redpanda.core.admin.v2.Broker"\x14\n\x12ListBrokersRequest"F\n\x13ListBrokersResponse\x12/\n\x07brokers\x18\x01 \x03(\x0b2\x1e.redpanda.core.admin.v2.Broker"\x8b\x01\n\x06Broker\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x125\n\nbuild_info\x18\x02 \x01(\x0b2!.redpanda.core.admin.v2.BuildInfo\x129\n\x0cadmin_server\x18\x03 \x01(\x0b2#.redpanda.core.admin.v2.AdminServer"/\n\tBuildInfo\x12\x0f\n\x07version\x18\x01 \x01(\t\x12\x11\n\tbuild_sha\x18\x02 \x01(\t"?\n\x0bAdminServer\x120\n\x06routes\x18\x01 \x03(\x0b2 .redpanda.core.admin.v2.RPCRoute",\n\x08RPCRoute\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x12\n\nhttp_route\x18\x02 \x01(\t2\xe9\x01\n\rBrokerService\x12h\n\tGetBroker\x12(.redpanda.core.admin.v2.GetBrokerRequest\x1a).redpanda.core.admin.v2.GetBrokerResponse"\x06\xea\x92\x19\x02\x10\x03\x12n\n\x0bListBrokers\x12*.redpanda.core.admin.v2.ListBrokersRequest\x1a+.redpanda.core.admin.v2.ListBrokersResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.broker_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_BROKERSERVICE'].methods_by_name['GetBroker']._loaded_options = None
    _globals['_BROKERSERVICE'].methods_by_name['GetBroker']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_BROKERSERVICE'].methods_by_name['ListBrokers']._loaded_options = None
    _globals['_BROKERSERVICE'].methods_by_name['ListBrokers']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_GETBROKERREQUEST']._serialized_start = 137
    _globals['_GETBROKERREQUEST']._serialized_end = 172
    _globals['_GETBROKERRESPONSE']._serialized_start = 174
    _globals['_GETBROKERRESPONSE']._serialized_end = 241
    _globals['_LISTBROKERSREQUEST']._serialized_start = 243
    _globals['_LISTBROKERSREQUEST']._serialized_end = 263
    _globals['_LISTBROKERSRESPONSE']._serialized_start = 265
    _globals['_LISTBROKERSRESPONSE']._serialized_end = 335
    _globals['_BROKER']._serialized_start = 338
    _globals['_BROKER']._serialized_end = 477
    _globals['_BUILDINFO']._serialized_start = 479
    _globals['_BUILDINFO']._serialized_end = 526
    _globals['_ADMINSERVER']._serialized_start = 528
    _globals['_ADMINSERVER']._serialized_end = 591
    _globals['_RPCROUTE']._serialized_start = 593
    _globals['_RPCROUTE']._serialized_end = 637
    _globals['_BROKERSERVICE']._serialized_start = 640
    _globals['_BROKERSERVICE']._serialized_end = 873