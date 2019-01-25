#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 19-1-21 下午6:47
"""
from functools import wraps

import aelog
import marshmallow
from sanic.request import Request

from .err_msg import http_msg
from .exceptions import FuncArgsError, HttpError

__all__ = ("singleton", "schema_validate")


def singleton(cls):
    """
    singleton for class of app
    """

    instances = {}

    @wraps(cls)
    def _singleton(*args, **kwargs):
        """
        singleton for class of app
        """
        cls_name = "_{0}".format(cls)
        if cls_name not in instances:
            instances[cls_name] = cls(*args, **kwargs)
        return instances[cls_name]

    return _singleton


def schema_validate(schema_obj, required: (tuple, list) = tuple(), is_extends=True, excluded: (tuple, list) = tuple(),
                    use_zh=True):
    """
    校验post的json格式和类型是否正确
    Args:
        schema_obj: 定义的schema对象
        required: 需要标记require的字段
        excluded: 排除不需要的字段
        is_extends: 是否继承schemea本身其他字段的require属性， 默认继承
        use_zh: 消息提示是否使用中文，默认中文
    Returns:
    """

    if not issubclass(schema_obj, marshmallow.Schema):
        raise FuncArgsError(message="schema_obj type error!")
    if not isinstance(required, (tuple, list)):
        raise FuncArgsError(message="required type error!")
    if not isinstance(excluded, (tuple, list)):
        raise FuncArgsError(message="excluded type error!")
    msg_zh = "msg_zh" if use_zh else "msg_en"

    def _validated(func):
        """
        校验post的json格式和类型是否正确
        """

        @wraps(func)
        async def _wrapper(*args, **kwargs):
            """
            校验post的json格式和类型是否正确
            """
            request = args[0] if isinstance(args[0], Request) else args[1]
            new_schema_obj = schema_obj(unknown="EXCLUDE")
            if required:
                for key, val in new_schema_obj.fields.items():
                    if key in required:  # 反序列化期间，把特别需要的字段标记为required
                        setattr(new_schema_obj.fields[key], "required", True)
                        setattr(new_schema_obj.fields[key], "dump_only", False)
                    elif not is_extends:
                        setattr(new_schema_obj.fields[key], "required", False)
            try:
                valid_data = new_schema_obj.load(request.json, unknown="EXCLUDE")
                # 把load后不需要的字段过滤掉，主要用于不允许修改的字段load后过滤掉
                for val in excluded:
                    valid_data.pop(val, None)
            except marshmallow.ValidationError as err:
                # 异常退出
                aelog.exception('Request body validation error, please check! {} {} error={}'.format(
                    request.method, request.path, err.messages))
                raise HttpError(400, message=http_msg[201][msg_zh], error=err.messages)
            except Exception as err:
                aelog.exception("Request body validation unknow error, please check!. {} {} error={}".format(
                    request.method, request.path, str(err)))
                raise HttpError(500, message=http_msg[202][msg_zh], error=str(err))
            else:
                request["json"] = valid_data
            return await func(*args, **kwargs)

        return _wrapper

    return _validated
