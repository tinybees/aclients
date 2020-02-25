#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/2/21 下午1:47
"""
import asyncio

from sanic import Sanic

from aclients.jsonrpc import SanicJsonRPC

app = Sanic()
jsonrpc = SanicJsonRPC(app)


@jsonrpc.jrpc
async def sub(a: int, b: int) -> int:
    await asyncio.sleep(0.1)
    return a - b


@jsonrpc.jrpc
async def test() -> str:
    await asyncio.sleep(0.1)
    return "中文"


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000)
