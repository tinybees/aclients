#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 19-4-22 下午11:35

实现简单的信号，用于业务解耦
"""

from sanic import Sanic

from aclients.utils import Cached

__all__ = ("Signal",)


class Signal(Cached):
    """
    异步信号实现
    """

    def __init__(self, signal_name):
        """
            异步信号实现
        Args:
            signal_name: 信号名称

        """
        self.signal_name = signal_name
        self.receiver = []

    def connect(self, receiver):
        """
        连接信号的订阅者
        Args:
            receiver: 信号订阅者
        Returns:

        """
        self.receiver.append(receiver)

    def disconnect(self, receiver):
        """
        取消连接信号的订阅者
        Args:
            receiver: 信号订阅者
        Returns:

        """
        self.receiver.remove(receiver)

    def send(self, app: Sanic, **kwargs):
        """
        发出信号到信号的订阅者，订阅者执行各自的功能
        Args:
            app: sanic的应用
            kwargs: 订阅者执行需要的参数
        Returns:

        """
        if not isinstance(app, Sanic):
            raise TypeError("app type must be Sanic type.")
        for func in self.receiver:
            app.loop.create_task(func(**kwargs))
        return app, kwargs
