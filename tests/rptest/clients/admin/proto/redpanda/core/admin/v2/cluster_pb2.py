"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/cluster.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
from ......proto.redpanda.core.admin.v2 import kafka_connections_pb2 as proto_dot_redpanda_dot_core_dot_admin_dot_v2_dot_kafka__connections__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n*proto/redpanda/core/admin/v2/cluster.proto\x12\x16redpanda.core.admin.v2\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a4proto/redpanda/core/admin/v2/kafka_connections.proto"R\n\x1bListKafkaConnectionsRequest\x12\x11\n\tpage_size\x18\x01 \x01(\x05\x12\x0e\n\x06filter\x18\x02 \x01(\t\x12\x10\n\x08order_by\x18\x03 \x01(\t"p\n\x1cListKafkaConnectionsResponse\x12<\n\x0bconnections\x18\x01 \x03(\x0b2\'.redpanda.core.admin.v2.KafkaConnection\x12\x12\n\ntotal_size\x18\x02 \x01(\x042\x9c\x01\n\x0eClusterService\x12\x89\x01\n\x14ListKafkaConnections\x123.redpanda.core.admin.v2.ListKafkaConnectionsRequest\x1a4.redpanda.core.admin.v2.ListKafkaConnectionsResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.cluster_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_CLUSTERSERVICE'].methods_by_name['ListKafkaConnections']._loaded_options = None
    _globals['_CLUSTERSERVICE'].methods_by_name['ListKafkaConnections']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LISTKAFKACONNECTIONSREQUEST']._serialized_start = 202
    _globals['_LISTKAFKACONNECTIONSREQUEST']._serialized_end = 284
    _globals['_LISTKAFKACONNECTIONSRESPONSE']._serialized_start = 286
    _globals['_LISTKAFKACONNECTIONSRESPONSE']._serialized_end = 398
    _globals['_CLUSTERSERVICE']._serialized_start = 401
    _globals['_CLUSTERSERVICE']._serialized_end = 557