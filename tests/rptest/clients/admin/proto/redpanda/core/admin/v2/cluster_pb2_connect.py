from __future__ import annotations
from collections.abc import AsyncIterator
from collections.abc import Iterator
from collections.abc import Iterable
import aiohttp
import urllib3
import typing
import sys
from connectrpc.client_async import AsyncConnectClient
from connectrpc.client_sync import ConnectClient
from connectrpc.client_protocol import ConnectProtocol
from connectrpc.client_connect import ConnectProtocolError
from connectrpc.headers import HeaderInput
from connectrpc.server import ClientRequest
from connectrpc.server import ClientStream
from connectrpc.server import ServerResponse
from connectrpc.server import ServerStream
from connectrpc.server_sync import ConnectWSGI
from connectrpc.streams import StreamInput
from connectrpc.streams import AsyncStreamOutput
from connectrpc.streams import StreamOutput
from connectrpc.unary import UnaryOutput
from connectrpc.unary import ClientStreamingOutput
if typing.TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from wsgiref.types import WSGIApplication
    else:
        from _typeshed.wsgi import WSGIApplication
from ...... import proto

class ClusterServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_list_kafka_connections(self, req: proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsResponse]:
        """Low-level method to call ListKafkaConnections, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ClusterService/ListKafkaConnections'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsResponse, extra_headers, timeout_seconds)

    def list_kafka_connections(self, req: proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsResponse:
        response = self.call_list_kafka_connections(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncClusterServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_list_kafka_connections(self, req: proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsResponse]:
        """Low-level method to call ListKafkaConnections, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ClusterService/ListKafkaConnections'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsResponse, extra_headers, timeout_seconds)

    async def list_kafka_connections(self, req: proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsResponse:
        response = await self.call_list_kafka_connections(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class ClusterServiceProtocol(typing.Protocol):

    def list_kafka_connections(self, req: ClientRequest[proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsResponse]:
        ...
CLUSTER_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.ClusterService'

def wsgi_cluster_service(implementation: ClusterServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.ClusterService/ListKafkaConnections', implementation.list_kafka_connections, proto.redpanda.core.admin.v2.cluster_pb2.ListKafkaConnectionsRequest)
    return app