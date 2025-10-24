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
from ....... import proto

class BreakglassServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_controller_forced_reconfiguration(self, req: proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationResponse]:
        """Low-level method to call ControllerForcedReconfiguration, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.v1.BreakglassService/ControllerForcedReconfiguration'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationResponse, extra_headers, timeout_seconds)

    def controller_forced_reconfiguration(self, req: proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationResponse:
        response = self.call_controller_forced_reconfiguration(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncBreakglassServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_controller_forced_reconfiguration(self, req: proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationResponse]:
        """Low-level method to call ControllerForcedReconfiguration, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.v1.BreakglassService/ControllerForcedReconfiguration'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationResponse, extra_headers, timeout_seconds)

    async def controller_forced_reconfiguration(self, req: proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationResponse:
        response = await self.call_controller_forced_reconfiguration(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class BreakglassServiceProtocol(typing.Protocol):

    def controller_forced_reconfiguration(self, req: ClientRequest[proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationResponse]:
        ...
BREAKGLASS_SERVICE_PATH_PREFIX = '/redpanda.core.admin.internal.v1.BreakglassService'

def wsgi_breakglass_service(implementation: BreakglassServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.internal.v1.BreakglassService/ControllerForcedReconfiguration', implementation.controller_forced_reconfiguration, proto.redpanda.core.admin.internal.v1.breakglass_pb2.ControllerForcedReconfigurationRequest)
    return app