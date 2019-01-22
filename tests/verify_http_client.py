#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-10-2 上午10:08
"""

import asyncio
import atexit

from aclients import AIOHttpClient
from aclients.exceptions import ClientResponseError

requests = AIOHttpClient()


async def get_verify(url, params, headers):
    """

    Args:

    Returns:

    """
    text = await requests.async_get(url, params=params, headers=headers)
    print("get", text.json())
    print("header", text.headers)
    print(text.headers["Connection"])
    assert text.json()["args"] == params
    assert list(headers.values())[0] in text.json()["headers"].values()


async def get_verify_error(url, params, headers):
    """

    Args:

    Returns:

    """
    try:
        await requests.async_get(url, params=params, headers=headers)
    except ClientResponseError as e:
        assert isinstance(e, ClientResponseError)


async def post_verify_data(url, data, headers):
    """

    Args:

    Returns:

    """
    text = await requests.async_post(url, data=data, headers=headers)
    print("post_data", text.json())
    assert text.json()["data"] == data
    assert list(headers.values())[0] in text.json()["headers"].values()


async def post_verify_json(url, json, headers):
    """

    Args:

    Returns:

    """
    text = await requests.async_post(url, json=json, headers=headers)
    print("post_json", text.json())
    assert text.json()["json"] == json
    assert list(headers.values())[0] in text.json()["headers"].values()


async def put_verify_data(url, data, headers):
    """

    Args:

    Returns:

    """
    text = await requests.async_put(url, data=data, headers=headers)
    print("put_data", text.json())
    assert text.json()["data"] == data
    assert list(headers.values())[0] in text.json()["headers"].values()


async def put_verify_json(url, json, headers):
    """

    Args:

    Returns:

    """
    text = await requests.async_put(url, json=json, headers=headers)
    print("put_json", text.json())
    assert text.json()["json"] == json
    assert list(headers.values())[0] in text.json()["headers"].values()


async def patch_verify_data(url, data, headers):
    """

    Args:

    Returns:

    """
    text = await requests.async_patch(url, data=data, headers=headers)
    print("patch_data", text.json())
    assert text.json()["data"] == data
    assert list(headers.values())[0] in text.json()["headers"].values()


async def patch_verify_json(url, json, headers):
    """

    Args:

    Returns:

    """
    text = await requests.async_patch(url, json=json, headers=headers)
    print("patch_json", text.json())
    assert text.json()["json"] == json
    assert list(headers.values())[0] in text.json()["headers"].values()


async def delete_verify(url):
    """

    Args:

    Returns:

    """
    text = await requests.async_delete(url)
    print("delete", text.json())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    atexit.register(loop.close)
    requests.init_session()
    tasks = [get_verify("http://httpbin.org/get", params={"test": "get"}, headers={"test": "header"}),
             get_verify_error("http://httpbin.org/get344", params={"test": "get"}, headers={"test": "header"}),
             post_verify_data("http://httpbin.org/post", data="post data", headers={"test": "header data"}),
             post_verify_json("http://httpbin.org/post", json={"test": "post json"}, headers={"test": "header json"}),
             put_verify_data("http://httpbin.org/put", data="put data", headers={"test": "header data"}),
             put_verify_json("http://httpbin.org/put", json={"test": "put json"}, headers={"test": "header json"}),
             patch_verify_data("http://httpbin.org/patch", data="patch data", headers={"test": "header patch"}),
             patch_verify_json("http://httpbin.org/patch", json={"test": "patch json"}, headers={"test": "header "
                                                                                                         "patch"}),
             delete_verify("http://httpbin.org/delete")]
    loop.run_until_complete(asyncio.wait(tasks))
