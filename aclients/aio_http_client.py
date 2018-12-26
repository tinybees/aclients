#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-26 上午11:49
"""
import asyncio

import aelog
import aiohttp

from aclients.err_msg import http_msg
from aclients.utils import verify_message
from .exceptions import ClientConnectionError, ClientError, ClientResponseError, HttpError

__all__ = ("AIOHttpClient", "AsyncResponse")


class AIOHttpClient(object):
    """
    基于aiohttp的异步封装
    """

    def __init__(self, app=None, *, timeout=5 * 60, verify_ssl=True, message=None, use_zh=True):
        """
            基于aiohttp的异步封装
        Args:
            app: app应用
            timeout:request timeout
            verify_ssl:verify ssl
            message: 提示消息
            use_zh: 消息提示是否使用中文，默认中文
        """
        self.session = None
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.message = None
        self.msg_zh = None

        if app is not None:
            self.init_app(app, timeout=timeout, verify_ssl=verify_ssl, message=message, use_zh=use_zh)

    def init_app(self, app, *, timeout=5 * 60, verify_ssl=True, message=None, use_zh=True):
        """
        基于aiohttp的异步封装
        Args:
            app: app应用
            timeout:request timeout
            verify_ssl:verify ssl
            message: 提示消息
            use_zh: 消息提示是否使用中文，默认中文
        Returns:

        """
        self.timeout = app.config.get("ACLIENTS_HTTP_TIMEOUT", None) or timeout
        self.verify_ssl = app.config.get("ACLIENTS_HTTP_VERIFYSSL", None) or verify_ssl
        message = app.config.get("ACLIENTS_HTTP_MESSAGE", None) or message
        use_zh = app.config.get("ACLIENTS_HTTP_MSGZH", None) or use_zh
        self.message = verify_message(http_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"

        @app.listener('before_server_start')
        def open_connection():
            """

            Args:

            Returns:

            """
            self.session = aiohttp.ClientSession()

        @app.listener('after_server_stop')
        async def close_connection():
            """
            释放redis连接池所有连接
            Args:

            Returns:

            """
            await self.session.close()

    async def _request(self, method, url, *, params=None, data=None, json=None, headers=None, timeout=None,
                       verify_ssl=None, **kwargs):
        """

        Args:
            method, url, *,  params=None, data=None, json=None, headers=None, **kwargs
        Returns:

        """

        async def _async_get():
            """

            Args:

            Returns:

            """
            return await self.session.get(url, params=params, headers=headers, timeout=timeout, verify_ssl=verify_ssl,
                                          **kwargs)

        async def _async_post():
            """

            Args:

            Returns:

            """
            res = await self.session.post(url, params=params, data=data, json=json, headers=headers, timeout=timeout,
                                          verify_ssl=verify_ssl, **kwargs)
            return res

        async def _async_put():
            """

            Args:

            Returns:

            """
            return await self.session.put(url, params=params, data=data, json=json, headers=headers, timeout=timeout,
                                          verify_ssl=verify_ssl, **kwargs)

        async def _async_patch():
            """

            Args:

            Returns:

            """
            return await self.session.patch(url, params=params, data=data, json=json, headers=headers, timeout=timeout,
                                            verify_ssl=verify_ssl, **kwargs)

        async def _async_delete():
            """

            Args:

            Returns:

            """
            return await self.session.delete(url, params=params, data=data, json=json, headers=headers, timeout=timeout,
                                             verify_ssl=verify_ssl, **kwargs)

        get_resp = {"GET": _async_get, "POST": _async_post, "PUT": _async_put, "DELETE": _async_delete,
                    "PATCH": _async_patch}
        resp = None
        try:
            resp = await get_resp[method.upper()]()
            resp.raise_for_status()
        except KeyError as e:
            raise ClientError(url=url, message="error method {0}".format(str(e)))
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
            raise ClientConnectionError(url=url, message=str(e))
        except aiohttp.ClientResponseError as e:
            try:
                resp_data = await resp.json() if resp else ""
            except (ValueError, TypeError, aiohttp.ContentTypeError):
                resp_data = await resp.text() if resp else ""
            raise ClientResponseError(url=url, status_code=e.status, message=e.message, headers=e.headers,
                                      body=resp_data)
        except aiohttp.ClientError as e:
            raise ClientError(url=url, message="aiohttp.ClientError: {}".format(vars(e)))

        async with resp:
            try:
                resp_json = await resp.json()
            except (ValueError, TypeError, aiohttp.ContentTypeError):
                try:
                    resp_text = await resp.text()
                except (ValueError, TypeError):
                    try:
                        resp_bytes = await resp.read()
                    except (aiohttp.ClientResponseError, aiohttp.ClientError) as e:
                        aelog.exception(e)
                        raise HttpError(e.code, message=self.message[200][self.msg_zh])
                    else:
                        return AsyncResponse(resp.status, resp.reason, resp.headers, resp.cookies, resp_body="",
                                             content=resp_bytes)
                else:
                    return AsyncResponse(resp.status, resp.reason, resp.headers, resp.cookies, resp_body=resp_text,
                                         content=b"")
            else:
                return AsyncResponse(resp.status, resp.reason, resp.headers, resp.cookies, resp_body=resp_json,
                                     content=b"")

    async def async_request(self, method, url, *, params=None, data=None, json=None, headers=None, timeout=None,
                            verify_ssl=None, **kwargs):
        """

        Args:

        Returns:

        """
        verify_ssl = self.verify_ssl if verify_ssl is None else verify_ssl
        timeout = self.timeout if timeout is None else timeout
        return await self._request(method, url, params=params, data=data, json=json, headers=headers,
                                   timeout=timeout, verify_ssl=verify_ssl, **kwargs)

    async def async_get(self, url, *, params=None, headers=None, timeout=None, verify_ssl=None, **kwargs):
        """

        Args:

        Returns:

        """
        verify_ssl = self.verify_ssl if verify_ssl is None else verify_ssl
        timeout = self.timeout if timeout is None else timeout
        return await self._request("GET", url, params=params, headers=headers, timeout=timeout, verify_ssl=verify_ssl,
                                   **kwargs)

    async def async_post(self, url, *, params=None, data=None, json=None, headers=None, timeout=None, verify_ssl=None,
                         **kwargs):
        """

        Args:

        Returns:

        """
        verify_ssl = self.verify_ssl if verify_ssl is None else verify_ssl
        timeout = self.timeout if timeout is None else timeout
        return await self._request("POST", url, params=params, data=data, json=json, headers=headers, timeout=timeout,
                                   verify_ssl=verify_ssl, **kwargs)

    async def async_put(self, url, *, params=None, data=None, json=None, headers=None, timeout=None, verify_ssl=None,
                        **kwargs):
        """

        Args:

        Returns:

        """
        verify_ssl = self.verify_ssl if verify_ssl is None else verify_ssl
        timeout = self.timeout if timeout is None else timeout
        return await self._request("PUT", url, params=params, data=data, json=json, headers=headers, timeout=timeout,
                                   verify_ssl=verify_ssl, **kwargs)

    async def async_patch(self, url, *, params=None, data=None, json=None, headers=None, timeout=None, verify_ssl=None,
                          **kwargs):
        """

        Args:

        Returns:

        """
        verify_ssl = self.verify_ssl if verify_ssl is None else verify_ssl
        timeout = self.timeout if timeout is None else timeout
        return await self._request("PATCH", url, params=params, data=data, json=json, headers=headers, timeout=timeout,
                                   verify_ssl=verify_ssl, **kwargs)

    async def async_delete(self, url, *, params=None, data=None, json=None, headers=None, verify_ssl=None, timeout=None,
                           **kwargs):
        """

        Args:

        Returns:

        """
        verify_ssl = self.verify_ssl if verify_ssl is None else verify_ssl
        timeout = self.timeout if timeout is None else timeout
        return await self._request("DELETE", url, params=params, data=data, json=json, headers=headers,
                                   timeout=timeout, verify_ssl=verify_ssl, **kwargs)


class AsyncResponse(object):
    """
    异步响应对象,需要重新封装对象
    """
    __slots__ = ["status_code", "reason", "headers", "cookies", "resp_body", "content"]

    def __init__(self, status_code, reason, headers, cookies, *, resp_body, content):
        """

        Args:

        """
        self.status_code = status_code
        self.reason = reason
        self.headers = headers
        self.cookies = cookies
        self.resp_body = resp_body
        self.content = content

    def json(self, ):
        """
        为了适配
        Args:

        Returns:

        """
        return self.resp_body
