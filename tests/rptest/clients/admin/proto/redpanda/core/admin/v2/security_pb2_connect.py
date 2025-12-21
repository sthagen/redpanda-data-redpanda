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

class SecurityServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_resolve_oidc_identity(self, req: proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse]:
        """Low-level method to call ResolveOidcIdentity, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ResolveOidcIdentity'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse, extra_headers, timeout_seconds)

    def resolve_oidc_identity(self, req: proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse:
        response = self.call_resolve_oidc_identity(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_refresh_oidc_keys(self, req: proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse]:
        """Low-level method to call RefreshOidcKeys, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RefreshOidcKeys'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse, extra_headers, timeout_seconds)

    def refresh_oidc_keys(self, req: proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse:
        response = self.call_refresh_oidc_keys(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_revoke_oidc_sessions(self, req: proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse]:
        """Low-level method to call RevokeOidcSessions, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RevokeOidcSessions'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse, extra_headers, timeout_seconds)

    def revoke_oidc_sessions(self, req: proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse:
        response = self.call_revoke_oidc_sessions(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncSecurityServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_resolve_oidc_identity(self, req: proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse]:
        """Low-level method to call ResolveOidcIdentity, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ResolveOidcIdentity'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse, extra_headers, timeout_seconds)

    async def resolve_oidc_identity(self, req: proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse:
        response = await self.call_resolve_oidc_identity(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_refresh_oidc_keys(self, req: proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse]:
        """Low-level method to call RefreshOidcKeys, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RefreshOidcKeys'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse, extra_headers, timeout_seconds)

    async def refresh_oidc_keys(self, req: proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse:
        response = await self.call_refresh_oidc_keys(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_revoke_oidc_sessions(self, req: proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse]:
        """Low-level method to call RevokeOidcSessions, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RevokeOidcSessions'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse, extra_headers, timeout_seconds)

    async def revoke_oidc_sessions(self, req: proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse:
        response = await self.call_revoke_oidc_sessions(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class SecurityServiceProtocol(typing.Protocol):

    def resolve_oidc_identity(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse]:
        ...

    def refresh_oidc_keys(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse]:
        ...

    def revoke_oidc_sessions(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse]:
        ...
SECURITY_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.SecurityService'

def wsgi_security_service(implementation: SecurityServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/ResolveOidcIdentity', implementation.resolve_oidc_identity, proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/RefreshOidcKeys', implementation.refresh_oidc_keys, proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/RevokeOidcSessions', implementation.revoke_oidc_sessions, proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest)
    return app