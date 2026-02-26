from typing import Callable
from ducktape.utils.util import wait_until
from connectrpc.errors import ConnectError, ConnectErrorCode
from rptest.clients.admin.v2 import Admin, metastore_pb, ntp_pb


def get_l1_partition_size(admin: Admin, topic: str, partition: int) -> int | None:
    """
    Returns the partition size in bytes, or None if the partition
    is not found in the metastore.
    """
    metastore = admin.metastore()
    req = metastore_pb.GetSizeRequest(
        partition=ntp_pb.TopicPartition(topic=topic, partition=partition)
    )
    try:
        response = metastore.get_size(req=req)
        return response.size_bytes
    except ConnectError as e:
        if e.code == ConnectErrorCode.NOT_FOUND:
            return None
        raise


def wait_until_l1_partition_size(
    admin: Admin,
    topic: str,
    partition: int,
    size_cond: Callable[[int], bool],
    timeout_sec: int = 60,
    backoff_sec: int = 5,
):
    """
    Wait until the size of the specificed partition in L1 passes the size_cond
    evaluation parameter. If the partition doesn't exist 0 is evaluated.
    """
    last_size: list[None | int] = [None]

    def pred():
        size = get_l1_partition_size(admin, topic, partition)
        last_size[0] = size
        return size_cond(size or 0)

    wait_until(
        condition=pred,
        timeout_sec=timeout_sec,
        backoff_sec=backoff_sec,
        err_msg=f"Waiting for L1 partition size. Last size {last_size[0]}",
        retry_on_exc=True,
    )
