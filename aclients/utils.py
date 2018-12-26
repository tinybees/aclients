#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-26 下午3:32
"""
from collections import MutableMapping, MutableSequence
from contextlib import contextmanager

import aelog

__all__ = ("ignore_error", "verify_message")


@contextmanager
def ignore_error(error=Exception):
    """
    个别情况下会忽略遇到的错误
    Args:

    Returns:

    """
    try:
        yield
    except error as e:
        aelog.warning(e)


def verify_message(src_message: dict, message: list or dict):
    """
    对用户提供的message进行校验
    Args:
        src_message: 默认提供的消息内容
        message: 指定的消息内容
    Returns:

    """
    src_message = dict(src_message)
    message = message if isinstance(message, MutableSequence) else [message]
    required_field = {"msg_code", "msg_zh", "msg_en"}

    for msg in message:
        if isinstance(msg, MutableMapping):
            if set(msg.keys()).intersection(required_field) == required_field and msg["msg_code"] in src_message:
                src_message[msg["msg_code"]].update(msg)
    return src_message
