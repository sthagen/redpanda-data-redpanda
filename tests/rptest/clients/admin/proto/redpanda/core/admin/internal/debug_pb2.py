"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/debug.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.pbgen import options_pb2 as proto_dot_redpanda_dot_pbgen_dot_options__pb2
from ......proto.redpanda.pbgen import rpc_pb2 as proto_dot_redpanda_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n.proto/redpanda/core/admin/internal/debug.proto\x12\x1credpanda.core.admin.internal\x1a"proto/redpanda/pbgen/options.proto\x1a\x1eproto/redpanda/pbgen/rpc.proto"\xdd\x01\n\x17StartStressFiberRequest\x12&\n\x1emin_spins_per_scheduling_point\x18\x01 \x01(\x05\x12&\n\x1emax_spins_per_scheduling_point\x18\x02 \x01(\x05\x12\x13\n\x0bstack_depth\x18\x03 \x01(\x05\x12#\n\x1bmin_ms_per_scheduling_point\x18\x04 \x01(\x05\x12#\n\x1bmax_ms_per_scheduling_point\x18\x05 \x01(\x05\x12\x13\n\x0bfiber_count\x18\x06 \x01(\x05"\x1a\n\x18StartStressFiberResponse"\x18\n\x16StopStressFiberRequest"\x19\n\x17StopStressFiberResponse"\xd2\x01\n\x1fThrowStructuredExceptionRequest\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12\x0e\n\x06reason\x18\x02 \x01(\t\x12]\n\x08metadata\x18\x03 \x03(\x0b2K.redpanda.core.admin.internal.ThrowStructuredExceptionRequest.MetadataEntry\x1a/\n\rMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x028\x01""\n ThrowStructuredExceptionResponse2\xc7\x03\n\x0cDebugService\x12\xa1\x01\n\x18ThrowStructuredException\x12=.redpanda.core.admin.internal.ThrowStructuredExceptionRequest\x1a>.redpanda.core.admin.internal.ThrowStructuredExceptionResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x89\x01\n\x10StartStressFiber\x125.redpanda.core.admin.internal.StartStressFiberRequest\x1a6.redpanda.core.admin.internal.StartStressFiberResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x86\x01\n\x0fStopStressFiber\x124.redpanda.core.admin.internal.StopStressFiberRequest\x1a5.redpanda.core.admin.internal.StopStressFiberResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.debug_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST_METADATAENTRY']._loaded_options = None
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST_METADATAENTRY']._serialized_options = b'8\x01'
    _globals['_DEBUGSERVICE'].methods_by_name['ThrowStructuredException']._loaded_options = None
    _globals['_DEBUGSERVICE'].methods_by_name['ThrowStructuredException']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_DEBUGSERVICE'].methods_by_name['StartStressFiber']._loaded_options = None
    _globals['_DEBUGSERVICE'].methods_by_name['StartStressFiber']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_DEBUGSERVICE'].methods_by_name['StopStressFiber']._loaded_options = None
    _globals['_DEBUGSERVICE'].methods_by_name['StopStressFiber']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_STARTSTRESSFIBERREQUEST']._serialized_start = 149
    _globals['_STARTSTRESSFIBERREQUEST']._serialized_end = 370
    _globals['_STARTSTRESSFIBERRESPONSE']._serialized_start = 372
    _globals['_STARTSTRESSFIBERRESPONSE']._serialized_end = 398
    _globals['_STOPSTRESSFIBERREQUEST']._serialized_start = 400
    _globals['_STOPSTRESSFIBERREQUEST']._serialized_end = 424
    _globals['_STOPSTRESSFIBERRESPONSE']._serialized_start = 426
    _globals['_STOPSTRESSFIBERRESPONSE']._serialized_end = 451
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST']._serialized_start = 454
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST']._serialized_end = 664
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST_METADATAENTRY']._serialized_start = 617
    _globals['_THROWSTRUCTUREDEXCEPTIONREQUEST_METADATAENTRY']._serialized_end = 664
    _globals['_THROWSTRUCTUREDEXCEPTIONRESPONSE']._serialized_start = 666
    _globals['_THROWSTRUCTUREDEXCEPTIONRESPONSE']._serialized_end = 700
    _globals['_DEBUGSERVICE']._serialized_start = 703
    _globals['_DEBUGSERVICE']._serialized_end = 1158