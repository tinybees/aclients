#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午5:15
"""
import atexit
import secrets
import ujson
import uuid
from collections import MutableMapping

import aelog
import aredis
from aredis import RedisError

from .exceptions import RedisClientError
from .utils import ignore_error

__all__ = ("Session", "AIORedisClient")

EXPIRED = 24 * 60 * 60


class Session(object):
    """
    保存实际看结果的session实例
    Args:

    """

    def __init__(self, account_id, *, org_id=None, role_id=None, permission_id=None, **kwargs):
        self.account_id = account_id  # 账户ID
        self.session_id = secrets.token_urlsafe()  # session ID
        self.org_id = org_id or uuid.uuid4().hex  # 账户的组织结构在redis中的ID
        self.role_id = role_id or uuid.uuid4().hex  # 账户的角色在redis中的ID
        self.permission_id = permission_id or uuid.uuid4().hex  # 账户的权限在redis中的ID
        self.static_permission_id = uuid.uuid4().hex  # 账户的静态权限在redis中的ID
        self.dynamic_permission_id = uuid.uuid4().hex  # 账户的动态权限在redis中的ID
        self.page_id = uuid.uuid4().hex  # 账户的页面权限在redis中的ID
        self.page_menu_id = uuid.uuid4().hex  # 账户的页面菜单权限在redis中的ID
        for k, v in kwargs.items():
            setattr(self, k, v)


class AIORedisClient(object):
    """
    redis 非阻塞工具类
    """

    def __init__(self, app=None, *, host="127.0.0.1", port=6379, dbname=0, passwd="", pool_size=50):
        """
        redis 非阻塞工具类
        Args:
            app: app应用
            host:redis host
            port:redis port
            dbname: database name
            passwd: redis password
            pool_size: redis pool size
        """
        self.pool = None
        self.redis_db = None
        self.host = host
        self.port = port
        self.dbname = dbname
        self.passwd = passwd
        self.pool_size = pool_size

        if app is not None:
            self.init_app(app, host=self.host, port=self.port, dbname=self.dbname, passwd=self.passwd,
                          pool_size=self.pool_size)

    def init_app(self, app, *, host=None, port=None, dbname=None, passwd="", pool_size=None):
        """
        redis 非阻塞工具类
        Args:
            app: app应用
            host:redis host
            port:redis port
            dbname: database name
            passwd: redis password
            pool_size: redis pool size
        Returns:

        """
        host = host or app.config.get("ACLIENTS_REDIS_HOST", None) or self.host
        port = port or app.config.get("ACLIENTS_REDIS_PORT", None) or self.port
        dbname = dbname or app.config.get("ACLIENTS_REDIS_DBNAME", None) or self.dbname
        passwd = passwd or app.config.get("ACLIENTS_REDIS_PASSWD", None) or self.passwd
        pool_size = pool_size or app.config.get("ACLIENTS_REDIS_POOL_SIZE", None) or self.pool_size

        passwd = passwd if passwd is None else str(passwd)

        @app.listener('before_server_start')
        async def open_connection(app_, loop):
            """

            Args:

            Returns:

            """
            # 返回值都做了解码，应用层不需要再decode
            self.pool = aredis.ConnectionPool(host=host, port=port, db=dbname, password=passwd, decode_responses=True,
                                              max_connections=pool_size)
            self.redis_db = aredis.StrictRedis(connection_pool=self.pool, decode_responses=True)

        @app.listener('after_server_stop')
        async def close_connection(app_, loop):
            """
            释放redis连接池所有连接
            Args:

            Returns:

            """
            self.redis_db = None
            self.pool.disconnect()

    def init_engine(self, *, host=None, port=None, dbname=None, passwd="", pool_size=None):
        """
        redis 非阻塞工具类
        Args:
            host:redis host
            port:redis port
            dbname: database name
            passwd: redis password
            pool_size: redis pool size
        Returns:

        """
        host = host or self.host
        port = port or self.port
        dbname = dbname or self.dbname
        passwd = passwd or self.passwd
        pool_size = pool_size or self.pool_size

        passwd = passwd if passwd is None else str(passwd)
        # 返回值都做了解码，应用层不需要再decode
        self.pool = aredis.ConnectionPool(host=host, port=port, db=dbname, password=passwd, decode_responses=True,
                                          max_connections=pool_size)
        self.redis_db = aredis.StrictRedis(connection_pool=self.pool, decode_responses=True)

        @atexit.register
        def close_connection():
            """
            释放redis连接池所有连接
            Args:

            Returns:

            """
            self.redis_db = None
            self.pool.disconnect()

    async def save_session(self, session: Session, dump_responses=False, ex=EXPIRED):
        """
        利用hash map保存session
        Args:
            session: Session 实例
            dump_responses: 是否对每个键值进行dump
            ex: 过期时间，单位秒
        Returns:

        """
        session_data = dict(vars(session))
        # 是否对每个键值进行dump
        if dump_responses:
            hash_data = {}
            for hash_key, hash_val in session_data.items():
                if not isinstance(hash_val, str):
                    with ignore_error():
                        hash_val = ujson.dumps(hash_val)
                hash_data[hash_key] = hash_val
            session_data = hash_data

        try:
            if not await self.redis_db.hmset(session_data["session_id"], session_data):
                raise RedisClientError("save session failed, session_id={}".format(session_data["session_id"]))
            if not await self.redis_db.expire(session_data["session_id"], ex):
                raise RedisClientError("set session expire failed, session_id={}".format(session_data["session_id"]))
        except RedisError as e:
            aelog.exception("save session error: {}, {}".format(session.session_id, e))
            raise RedisClientError(str(e))
        else:
            return session.session_id

    async def delete_session(self, session_id):
        """
        利用hash map删除session
        Args:
            session_id: session id
        Returns:

        """

        try:
            session_id_ = await self.redis_db.hget(session_id, "session_id")
            if session_id_ != session_id:
                raise RedisClientError("invalid session_id, session_id={}".format(session_id))

            if not await self.redis_db.delete(session_id):
                raise RedisClientError("delete session failed, session_id={}".format(session_id))
        except RedisError as e:
            aelog.exception("delete session error: {}, {}".format(session_id, e))
            raise RedisClientError(str(e))

    async def update_session(self, session: Session, dump_responses=False, ex=EXPIRED):
        """
        利用hash map更新session
        Args:
            session: Session实例
            ex: 过期时间，单位秒
            dump_responses: 是否对每个键值进行dump
        Returns:

        """
        session_data = dict(vars(session))
        # 是否对每个键值进行dump
        if dump_responses:
            hash_data = {}
            for hash_key, hash_val in session_data.items():
                if not isinstance(hash_val, str):
                    with ignore_error():
                        hash_val = ujson.dumps(hash_val)
                hash_data[hash_key] = hash_val
            session_data = hash_data

        try:
            if not await self.redis_db.hmset(session_data["session_id"], session_data):
                raise RedisClientError("update session failed, session_id={}".format(session_data["session_id"]))
            if not await self.redis_db.expire(session_data["session_id"], ex):
                raise RedisClientError("set session expire failed, session_id={}".format(session_data["session_id"]))
        except RedisError as e:
            aelog.exception("update session error: {}, {}".format(session_data["session_id"], e))
            raise RedisClientError(str(e))

    async def get_session(self, session_id, ex=EXPIRED, cls_flag=True, load_responses=False) -> Session:
        """
        获取session
        Args:
            session_id: session id
            ex: 过期时间，单位秒
            cls_flag: 是否返回session的类实例
            load_responses: 结果的键值是否进行load
        Returns:

        """

        try:
            session_data = await self.redis_db.hgetall(session_id)
            if not session_data:
                raise RedisClientError("not found session, session_id={}".format(session_id))

            if not await self.redis_db.expire(session_id, ex):
                raise RedisClientError("set session expire failed, session_id={}".format(session_id))
        except RedisError as e:
            aelog.exception("get session error: {}, {}".format(session_id, e))
            raise RedisClientError(e)
        else:
            # 返回的键值对是否做load
            if load_responses:
                hash_data = {}
                for hash_key, hash_val in session_data.items():
                    with ignore_error():
                        hash_val = ujson.loads(hash_val)
                    hash_data[hash_key] = hash_val
                session_data = hash_data

            if cls_flag:
                return Session(account_id=session_data.pop('account_id'), org_id=session_data.pop("org_id"),
                               role_id=session_data.pop("role_id"), permission_id=session_data.pop("permission_id"),
                               **session_data)
            else:
                return session_data

    async def verify(self, session_id):
        """
        校验session，主要用于登录校验
        Args:
            session_id
        Returns:

        """
        try:
            session = await self.get_session(session_id)
        except RedisClientError as e:
            raise RedisClientError(str(e))
        else:
            if not session:
                raise RedisClientError("invalid session_id, session_id={}".format(session_id))
            return session

    async def save_update_hash_data(self, name, hash_data: dict, field_name=None, ex=EXPIRED, dump_responses=False):
        """
        获取hash对象field_name对应的值
        Args:
            name: redis hash key的名称
            field_name: 保存的hash mapping 中的某个字段
            hash_data: 获取的hash对象中属性的名称
            ex: 过期时间，单位秒
            dump_responses: 是否对每个键值进行dump
        Returns:
            反序列化对象
        """
        if not isinstance(hash_data, MutableMapping):
            raise ValueError("hash data error, must be MutableMapping.")

        try:
            if not field_name:
                # 是否对每个键值进行dump
                if dump_responses:
                    rs_data = {}
                    for hash_key, hash_val in hash_data.items():
                        if not isinstance(hash_val, str):
                            with ignore_error():
                                hash_val = ujson.dumps(hash_val)
                        rs_data[hash_key] = hash_val
                    hash_data = rs_data

                if not await self.redis_db.hmset(name, hash_data):
                    raise RedisClientError("save hash data mapping failed, session_id={}".format(name))
            else:
                hash_data = hash_data if isinstance(hash_data, str) else ujson.dumps(hash_data)
                await self.redis_db.hset(name, field_name, hash_data)

            if not await self.redis_db.expire(name, ex):
                raise RedisClientError("set hash data expire failed, session_id={}".format(name))
        except RedisError as e:
            raise RedisClientError(str(e))
        else:
            return name

    async def get_hash_data(self, name, field_name=None, ex=EXPIRED, load_responses=False):
        """
        获取hash对象field_name对应的值
        Args:
            name: redis hash key的名称
            field_name: 获取的hash对象中属性的名称
            ex: 过期时间，单位秒
            load_responses: 结果的键值是否进行load
        Returns:
            反序列化对象
        """
        try:
            if field_name:
                hash_data = await self.redis_db.hget(name, field_name)
                # 返回的键值对是否做load
                if load_responses:
                    with ignore_error():
                        hash_data = ujson.loads(hash_data)
            else:
                hash_data = await self.redis_db.hgetall(name)
                # 返回的键值对是否做load
                if load_responses:
                    rs_data = {}
                    for hash_key, hash_val in hash_data.items():
                        with ignore_error():
                            hash_val = ujson.loads(hash_val)
                        rs_data[hash_key] = hash_val
                    hash_data = rs_data
            if not hash_data:
                raise RedisClientError("not found hash data, name={}, field_name={}".format(name, field_name))

            if not await self.redis_db.expire(name, ex):
                raise RedisClientError("set expire failed, name={}".format(name))
        except RedisError as e:
            raise RedisClientError(str(e))
        else:
            return hash_data

    async def get_list_data(self, name, start=0, end=-1, ex=EXPIRED):
        """
        保存数据到redis的列表中
        Args:
            name: redis key的名称
            start: 获取数据的起始位置,默认列表的第一个值
            end: 获取数据的结束位置，默认列表的最后一个值
            ex: 过期时间，单位秒
        Returns:

        """
        try:
            data = await self.redis_db.lrange(name, start=start, end=end)
            if not data:
                raise RedisClientError("not found list data, name={}, start={}, end={}".format(name, start, end))

            if not await self.redis_db.expire(name, ex):
                raise RedisClientError("set expire failed, name={}".format(name))
        except RedisError as e:
            raise RedisClientError(str(e))
        else:
            return data

    async def save_list_data(self, name, list_data: list or str, save_to_left=True, ex=EXPIRED):
        """
        保存数据到redis的列表中
        Args:
            name: redis key的名称
            list_data: 保存的值,可以是单个值也可以是元祖
            save_to_left: 是否保存到列表的左边，默认保存到左边
            ex: 过期时间，单位秒
        Returns:

        """
        list_data = (list_data,) if isinstance(list_data, str) else list_data
        try:
            if save_to_left:
                if not await self.redis_db.lpush(name, *list_data):
                    raise RedisClientError("lpush value to head failed.")
            else:
                if not await self.redis_db.rpush(name, *list_data):
                    raise RedisClientError("lpush value to tail failed.")
            if not await self.redis_db.expire(name, ex):
                raise RedisClientError("set expire failed, name={}".format(name))
        except RedisError as e:
            raise RedisClientError(str(e))
        else:
            return name

    async def save_update_usual_data(self, name, value, ex=EXPIRED):
        """
        保存列表、映射对象为普通的字符串
        Args:
            name: redis key的名称
            value: 保存的值，可以是可序列化的任何职
            ex: 过期时间，单位秒
        Returns:

        """
        value = ujson.dumps(value) if not isinstance(value, str) else value
        try:
            if not await self.redis_db.set(name, value, ex):
                raise RedisClientError("set serializable value failed!")
        except RedisError as e:
            raise RedisClientError(str(e))
        else:
            return name

    async def get_usual_data(self, name, ex=EXPIRED):
        """
        获取name对应的值
        Args:
            name: redis key的名称
            ex: 过期时间，单位秒
        Returns:
            反序列化对象
        """
        try:
            data = await self.redis_db.get(name)
            if not data:
                raise RedisClientError("not found usual data, name={}".format(name))

            if not await self.redis_db.expire(name, ex):
                raise RedisClientError("set expire failed, name={}".format(name))

        except RedisError as e:
            raise RedisClientError(str(e))
        else:
            with ignore_error():
                data = ujson.loads(data)
            return data

    async def is_exist_key(self, name):
        """
        判断redis key是否存在
        Args:
            name: redis key的名称
        Returns:

        """
        try:
            data = await self.redis_db.exists(name)
        except RedisError as e:
            raise RedisClientError(str(e))
        else:
            return data

    async def delete_keys(self, names: list):
        """
        删除一个或多个redis key
        Args:
            names: redis key的名称
        Returns:

        """
        names = (names,) if isinstance(names, str) else names
        try:
            if not await self.redis_db.delete(*names):
                raise RedisClientError("Delete redis keys failed {}.".format(*names))
        except RedisError as e:
            raise RedisClientError(str(e))

    async def get_keys(self, pattern_name):
        """
        根据正则表达式获取redis的keys
        Args:
            pattern_name:正则表达式的名称
        Returns:

        """
        try:
            data = await self.redis_db.keys(pattern_name)
        except RedisError as e:
            raise RedisClientError(str(e))
        else:
            return data
