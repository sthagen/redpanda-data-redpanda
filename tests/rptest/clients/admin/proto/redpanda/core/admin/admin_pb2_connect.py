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
from ..... import proto

class AdminServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_get_build_info(self, req: proto.redpanda.core.admin.admin_pb2.GetBuildInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.admin_pb2.BuildInfo]:
        """Low-level method to call GetBuildInfo, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.AdminService/GetBuildInfo'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.admin_pb2.BuildInfo, extra_headers, timeout_seconds)

    def get_build_info(self, req: proto.redpanda.core.admin.admin_pb2.GetBuildInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.admin_pb2.BuildInfo:
        response = self.call_get_build_info(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_rpc_routes(self, req: proto.redpanda.core.admin.admin_pb2.ListRPCRoutesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.admin_pb2.ListRPCRoutesResponse]:
        """Low-level method to call ListRPCRoutes, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.AdminService/ListRPCRoutes'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.admin_pb2.ListRPCRoutesResponse, extra_headers, timeout_seconds)

    def list_rpc_routes(self, req: proto.redpanda.core.admin.admin_pb2.ListRPCRoutesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.admin_pb2.ListRPCRoutesResponse:
        response = self.call_list_rpc_routes(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncAdminServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_get_build_info(self, req: proto.redpanda.core.admin.admin_pb2.GetBuildInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.admin_pb2.BuildInfo]:
        """Low-level method to call GetBuildInfo, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.AdminService/GetBuildInfo'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.admin_pb2.BuildInfo, extra_headers, timeout_seconds)

    async def get_build_info(self, req: proto.redpanda.core.admin.admin_pb2.GetBuildInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.admin_pb2.BuildInfo:
        response = await self.call_get_build_info(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_rpc_routes(self, req: proto.redpanda.core.admin.admin_pb2.ListRPCRoutesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.admin_pb2.ListRPCRoutesResponse]:
        """Low-level method to call ListRPCRoutes, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.AdminService/ListRPCRoutes'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.admin_pb2.ListRPCRoutesResponse, extra_headers, timeout_seconds)

    async def list_rpc_routes(self, req: proto.redpanda.core.admin.admin_pb2.ListRPCRoutesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.admin_pb2.ListRPCRoutesResponse:
        response = await self.call_list_rpc_routes(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class AdminServiceProtocol(typing.Protocol):

    def get_build_info(self, req: ClientRequest[proto.redpanda.core.admin.admin_pb2.GetBuildInfoRequest]) -> ServerResponse[proto.redpanda.core.admin.admin_pb2.BuildInfo]:
        ...

    def list_rpc_routes(self, req: ClientRequest[proto.redpanda.core.admin.admin_pb2.ListRPCRoutesRequest]) -> ServerResponse[proto.redpanda.core.admin.admin_pb2.ListRPCRoutesResponse]:
        ...
ADMIN_SERVICE_PATH_PREFIX = '/redpanda.core.admin.AdminService'

def wsgi_admin_service(implementation: AdminServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.AdminService/GetBuildInfo', implementation.get_build_info, proto.redpanda.core.admin.admin_pb2.GetBuildInfoRequest)
    app.register_unary_rpc('/redpanda.core.admin.AdminService/ListRPCRoutes', implementation.list_rpc_routes, proto.redpanda.core.admin.admin_pb2.ListRPCRoutesRequest)
    return app