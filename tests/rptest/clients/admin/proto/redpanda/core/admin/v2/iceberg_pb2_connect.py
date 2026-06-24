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

class IcebergServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_get_iceberg_status(self, req: proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusResponse]:
        """Low-level method to call GetIcebergStatus, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.IcebergService/GetIcebergStatus'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusResponse, extra_headers, timeout_seconds)

    def get_iceberg_status(self, req: proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusResponse:
        response = self.call_get_iceberg_status(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncIcebergServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_get_iceberg_status(self, req: proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusResponse]:
        """Low-level method to call GetIcebergStatus, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.IcebergService/GetIcebergStatus'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusResponse, extra_headers, timeout_seconds)

    async def get_iceberg_status(self, req: proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusResponse:
        response = await self.call_get_iceberg_status(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class IcebergServiceProtocol(typing.Protocol):

    def get_iceberg_status(self, req: ClientRequest[proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusResponse]:
        ...
ICEBERG_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.IcebergService'

def wsgi_iceberg_service(implementation: IcebergServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.IcebergService/GetIcebergStatus', implementation.get_iceberg_status, proto.redpanda.core.admin.v2.iceberg_pb2.GetIcebergStatusRequest)
    return app