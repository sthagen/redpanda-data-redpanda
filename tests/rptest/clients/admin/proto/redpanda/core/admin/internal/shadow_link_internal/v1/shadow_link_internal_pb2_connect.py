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
from ........ import proto

class ShadowLinkInternalServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_remove_shadow_topic(self, req: proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicResponse]:
        """Low-level method to call RemoveShadowTopic, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.shadow_link.v1.ShadowLinkInternalService/RemoveShadowTopic'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicResponse, extra_headers, timeout_seconds)

    def remove_shadow_topic(self, req: proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicResponse:
        response = self.call_remove_shadow_topic(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_force_update_shadow_topic_state(self, req: proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateResponse]:
        """Low-level method to call ForceUpdateShadowTopicState, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.shadow_link.v1.ShadowLinkInternalService/ForceUpdateShadowTopicState'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateResponse, extra_headers, timeout_seconds)

    def force_update_shadow_topic_state(self, req: proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateResponse:
        response = self.call_force_update_shadow_topic_state(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncShadowLinkInternalServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_remove_shadow_topic(self, req: proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicResponse]:
        """Low-level method to call RemoveShadowTopic, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.shadow_link.v1.ShadowLinkInternalService/RemoveShadowTopic'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicResponse, extra_headers, timeout_seconds)

    async def remove_shadow_topic(self, req: proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicResponse:
        response = await self.call_remove_shadow_topic(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_force_update_shadow_topic_state(self, req: proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateResponse]:
        """Low-level method to call ForceUpdateShadowTopicState, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.shadow_link.v1.ShadowLinkInternalService/ForceUpdateShadowTopicState'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateResponse, extra_headers, timeout_seconds)

    async def force_update_shadow_topic_state(self, req: proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateResponse:
        response = await self.call_force_update_shadow_topic_state(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class ShadowLinkInternalServiceProtocol(typing.Protocol):

    def remove_shadow_topic(self, req: ClientRequest[proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicResponse]:
        ...

    def force_update_shadow_topic_state(self, req: ClientRequest[proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateResponse]:
        ...
SHADOW_LINK_INTERNAL_SERVICE_PATH_PREFIX = '/redpanda.core.admin.internal.shadow_link.v1.ShadowLinkInternalService'

def wsgi_shadow_link_internal_service(implementation: ShadowLinkInternalServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.internal.shadow_link.v1.ShadowLinkInternalService/RemoveShadowTopic', implementation.remove_shadow_topic, proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.RemoveShadowTopicRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.shadow_link.v1.ShadowLinkInternalService/ForceUpdateShadowTopicState', implementation.force_update_shadow_topic_state, proto.redpanda.core.admin.internal.shadow_link_internal.v1.shadow_link_internal_pb2.ForceUpdateShadowTopicStateRequest)
    return app