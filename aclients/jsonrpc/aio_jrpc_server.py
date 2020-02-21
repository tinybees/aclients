#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/2/21 下午5:00
"""
from typing import Optional

from sanic import Sanic

try:
    from sanic_jsonrpc import Jsonrpc
except ImportError as e:
    raise e

__all__ = ("SanicJsonRPC",)


class SanicJsonRPC(Jsonrpc):
    """

    """

    def __init__(self, app: Sanic):
        """

        Args:

        """
        post_route: Optional[str] = "/api/jrpc/post"
        ws_route: Optional[str] = "/api/jrpc/ws"
        super().__init__(app, post_route, ws_route)
