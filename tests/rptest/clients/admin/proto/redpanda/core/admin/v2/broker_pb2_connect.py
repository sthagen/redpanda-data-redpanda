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

class BrokerServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_get_broker(self, req: proto.redpanda.core.admin.v2.broker_pb2.GetBrokerRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.broker_pb2.GetBrokerResponse]:
        """Low-level method to call GetBroker, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.BrokerService/GetBroker'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.broker_pb2.GetBrokerResponse, extra_headers, timeout_seconds)

    def get_broker(self, req: proto.redpanda.core.admin.v2.broker_pb2.GetBrokerRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.broker_pb2.GetBrokerResponse:
        response = self.call_get_broker(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_brokers(self, req: proto.redpanda.core.admin.v2.broker_pb2.ListBrokersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.broker_pb2.ListBrokersResponse]:
        """Low-level method to call ListBrokers, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.BrokerService/ListBrokers'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.broker_pb2.ListBrokersResponse, extra_headers, timeout_seconds)

    def list_brokers(self, req: proto.redpanda.core.admin.v2.broker_pb2.ListBrokersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.broker_pb2.ListBrokersResponse:
        response = self.call_list_brokers(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncBrokerServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_get_broker(self, req: proto.redpanda.core.admin.v2.broker_pb2.GetBrokerRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.broker_pb2.GetBrokerResponse]:
        """Low-level method to call GetBroker, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.BrokerService/GetBroker'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.broker_pb2.GetBrokerResponse, extra_headers, timeout_seconds)

    async def get_broker(self, req: proto.redpanda.core.admin.v2.broker_pb2.GetBrokerRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.broker_pb2.GetBrokerResponse:
        response = await self.call_get_broker(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_brokers(self, req: proto.redpanda.core.admin.v2.broker_pb2.ListBrokersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.broker_pb2.ListBrokersResponse]:
        """Low-level method to call ListBrokers, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.BrokerService/ListBrokers'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.broker_pb2.ListBrokersResponse, extra_headers, timeout_seconds)

    async def list_brokers(self, req: proto.redpanda.core.admin.v2.broker_pb2.ListBrokersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.broker_pb2.ListBrokersResponse:
        response = await self.call_list_brokers(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class BrokerServiceProtocol(typing.Protocol):

    def get_broker(self, req: ClientRequest[proto.redpanda.core.admin.v2.broker_pb2.GetBrokerRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.broker_pb2.GetBrokerResponse]:
        ...

    def list_brokers(self, req: ClientRequest[proto.redpanda.core.admin.v2.broker_pb2.ListBrokersRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.broker_pb2.ListBrokersResponse]:
        ...
BROKER_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.BrokerService'

def wsgi_broker_service(implementation: BrokerServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.BrokerService/GetBroker', implementation.get_broker, proto.redpanda.core.admin.v2.broker_pb2.GetBrokerRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.BrokerService/ListBrokers', implementation.list_brokers, proto.redpanda.core.admin.v2.broker_pb2.ListBrokersRequest)
    return app