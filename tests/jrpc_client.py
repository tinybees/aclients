#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/2/21 下午2:14
"""
from aclients import AIOHttpClient
from aclients.jsonrpc import AIOJRPCClient


async def sub(client: AIOJRPCClient):
    """

    Args:

    Returns:

    """
    r = await client["local"].sub(5, 2).done()
    print(r)
    s = await client["local"].test().done()
    print(s)

    s2 = await client["local"].sub(5, 2).test(3).done()
    print(s2)


if __name__ == '__main__':
    aio_http = AIOHttpClient()
    aio_http.init_session()
    aio_jrpc = AIOJRPCClient(aio_http)
    aio_jrpc.register("local", ("127.0.0.1", 8000))
    aio_jrpc.register("loca2", ("127.0.0.1", 8001))
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(sub(aio_jrpc))
