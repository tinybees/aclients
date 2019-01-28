#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午2:08
"""
from sanic.exceptions import SanicException

__all__ = ("ClientError", "ClientResponseError", "ClientConnectionError", "HttpError", "RedisClientError",
           "RedisConnectError", "MysqlDuplicateKeyError", "MysqlError", "MysqlInvalidNameError", "FuncArgsError",
           "Error", "PermissionDeniedError", "QueryArgsError", "MongoError", "MongoDuplicateKeyError",
           "MongoInvalidNameError", "CommandArgsError", "EmailError", "ConfigError")


class Error(Exception):
    """
    异常基类
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        return "Error: message='{}'".format(self.message)

    def __repr__(self):
        return "<{} '{}'>".format(self.__class__.__name__, self.message)


class ClientError(Error):
    """
    主要处理异步请求的error
    """

    def __init__(self, url, *, message=None):
        self.url = url
        self.message = message
        super().__init__(message)

    def __str__(self):
        return "Error: url='{}', message='{}'".format(self.url, self.message)

    def __repr__(self):
        return "<{} '{}, {}'>".format(self.__class__.__name__, self.url, self.message)


class ClientResponseError(ClientError):
    """
    响应异常
    """

    def __init__(self, url, *, status_code=None, message=None, headers=None, body=None):
        self.url = url
        self.status_code = status_code
        self.message = message
        self.headers = headers
        self.body = body
        super().__init__(self.url, message=self.message)

    def __str__(self):
        return "Error: code={}, url='{}', message='{}', body='{}'".format(
            self.status_code, self.url, self.message, self.body)

    def __repr__(self):
        return "<{} '{}, {}, {}'>".format(self.__class__.__name__, self.status_code, self.url, self.message)


class ClientConnectionError(ClientError):
    """连接异常"""

    pass


class HttpError(Error, SanicException):
    """
    主要处理http 错误,从接口返回
    """

    def __init__(self, status_code, *, message=None, error=None):
        self.status_code = status_code
        self.message = message
        self.error = error
        super(SanicException, self).__init__(message, status_code)

    def __str__(self):
        return "{}, '{}':'{}'".format(self.status_code, self.message, self.message or self.error)

    def __repr__(self):
        return "<{} '{}: {}'>".format(self.__class__.__name__, self.status_code, self.error or self.message)


class RedisClientError(Error):
    """
    主要处理redis的error
    """

    pass


class RedisConnectError(RedisClientError):
    """
    主要处理redis的connect error
    """
    pass


class EmailError(Error):
    """
    主要处理email error
    """

    pass


class ConfigError(Error):
    """
    主要处理config error
    """

    pass


class MysqlError(Error):
    """
    主要处理mongo错误
    """

    pass


class MysqlDuplicateKeyError(MysqlError):
    """
    处理键重复引发的error
    """

    pass


class MysqlInvalidNameError(MysqlError):
    """
    处理名称错误引发的error
    """

    pass


class MongoError(Error):
    """
    主要处理mongo错误
    """

    pass


class MongoDuplicateKeyError(MongoError):
    """
    处理键重复引发的error
    """

    pass


class MongoInvalidNameError(MongoError):
    """
    处理名称错误引发的error
    """

    pass


class FuncArgsError(Error):
    """
    处理函数参数不匹配引发的error
    """

    pass


class PermissionDeniedError(Error):
    """
    处理权限被拒绝时的错误
    """

    pass


class QueryArgsError(Error):
    """
    处理salalemy 拼接query错误
    """

    pass


class CommandArgsError(Error):
    """
    处理执行命令时，命令失败错误
    """

    pass
