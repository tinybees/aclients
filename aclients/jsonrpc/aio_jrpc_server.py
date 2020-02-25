#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/2/21 下午5:00
"""

from sanic import Sanic

try:
    from sanic_jsonrpc import Jsonrpc
except ImportError as e:
    raise e

__all__ = ("SanicJsonRPC",)


class SanicJsonRPC(object):
    """
    sanic jsonrpc object
    """

    def __init__(self, app: Sanic = None, post_route: str = "/api/jrpc/post", ws_route: str = "/api/jrpc/ws"):
        """
        jsonrpc 实例初始化
        Args:
            app: app应用
            post_route: post url
            ws_route: websocket url

        """
        self.jrpc: Jsonrpc = None
        self.post_route: str = post_route
        self.ws_route: str = ws_route

        if app is not None:
            self.init_app(app)

    def init_app(self, app: Sanic = None, post_route: str = "/api/jrpc/post", ws_route: str = "/api/jrpc/ws"):
        """
        jsonrpc 实例初始化
        Args:
            app: app应用
            post_route: post url
            ws_route: websocket url
        Returns:

        """
        self.post_route = post_route or self.post_route
        self.ws_route = ws_route or self.ws_route

        self.jrpc = Jsonrpc(app, post_route=self.post_route, ws_route=self.ws_route)
