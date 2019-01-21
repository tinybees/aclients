#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午2:08
"""

__all__ = ("ClientError", "ClientResponseError", "ClientConnectionError", "HttpError", "RedisClientError",
           "RedisConnectError", "MysqlDuplicateKeyError", "MysqlError", "MysqlInvalidNameError", "FuncArgsError",
           "Error", "PermissionDeniedError", "QueryArgsError", "MongoError", "MongoDuplicateKeyError",
           "MongoInvalidNameError", "CommandArgsError")


class Error(Exception):
    """
    异常基类
    """


class ClientError(Error):
    """
    主要处理异步请求的error
    """

    def __init__(self, url, *, message=None):
        self.url = url
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: url='{0}', message='{1}'".format(self.url, self.message)

    __repr__ = __str__


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
        super().__init__(url=self.url, message=self.message)

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: code={0}, url={1}, message='{2}', body={3}".format(self.status_code, self.url, self.message,
                                                                          self.body)


class ClientConnectionError(ClientError):
    """连接异常"""

    def __init__(self, url, *, message=None):
        self.url = url
        self.message = message
        super().__init__(url=self.url, message=self.message)


class HttpError(Error):
    """
    主要处理http 错误,从接口返回
    """

    def __init__(self, status_code, *, message=None, error=None):
        self.status_code = status_code
        self.message = message
        self.error = error


class RedisClientError(Error):
    """
    主要处理redis的error
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class RedisConnectError(RedisClientError):
    """
    主要处理redis的connect error
    """


class EmailError(Error):
    """
    主要处理email error
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class ConfigError(Error):
    """
    主要处理config error
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class MysqlError(Error):
    """
    主要处理mongo错误
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class MysqlDuplicateKeyError(MysqlError):
    """
    处理键重复引发的error
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class MysqlInvalidNameError(MysqlError):
    """
    处理名称错误引发的error
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class MongoError(Error):
    """
    主要处理mongo错误
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class MongoDuplicateKeyError(MongoError):
    """
    处理键重复引发的error
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class MongoInvalidNameError(MongoError):
    """
    处理名称错误引发的error
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class FuncArgsError(Error):
    """
    处理函数参数不匹配引发的error
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class PermissionDeniedError(Error):
    """
    处理权限被拒绝时的错误
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class QueryArgsError(Error):
    """
    处理salalemy 拼接query错误
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__


class CommandArgsError(Error):
    """
    处理执行命令时，命令失败错误
    """

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        """

        Args:

        Returns:

        """
        return "Error: message='{0}'".format(self.message)

    __repr__ = __str__
