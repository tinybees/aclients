#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午4:58
"""
import asyncio
import atexit
from collections import MutableMapping, MutableSequence

import aelog
from aiomysql.sa import create_engine
from aiomysql.sa.exc import Error
from pymysql.err import IntegrityError, MySQLError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import delete, desc, func, insert, or_, select, update

from .err_msg import mysql_msg
from .exceptions import FuncArgsError, HttpError, MysqlDuplicateKeyError, MysqlError, QueryArgsError
from .utils import verify_message

__all__ = ("AIOMysqlClient",)


class AIOMysqlClient(object):
    """
    MySQL异步操作指南
    """

    def __init__(self, app=None, *, username="root", passwd=None, host="127.0.0.1", port=3306, dbname=None,
                 pool_size=50, **kwargs):
        """
        mongo 非阻塞工具类
        Args:
            app: app应用
            host:mysql host
            port:mysql port
            dbname: database name
            username: mysql user
            passwd: mysql password
            pool_size: mysql pool size
        """

        self.aio_engine = None
        self.username = username
        self.passwd = passwd
        self.host = host
        self.port = port
        self.dbname = dbname
        self.pool_size = pool_size
        self.message = kwargs.get("message", {})
        self.use_zh = kwargs.get("use_zh", True)
        self.msg_zh = None

        if app is not None:
            self.init_app(app, username=self.username, passwd=self.passwd, host=self.host, port=self.port,
                          dbname=self.dbname, pool_size=self.pool_size, **kwargs)

    def init_app(self, app, *, username=None, passwd=None, host=None, port=None, dbname=None,
                 pool_size=None, **kwargs):
        """
        mysql 实例初始化
        Args:
            app: app应用
            host:mysql host
            port:mysql port
            dbname: database name
            username: mysql user
            passwd: mysql password
            pool_size: mysql pool size

        Returns:

        """
        username = username or app.config.get("ACLIENTS_MYSQL_USERNAME", None) or self.username
        passwd = passwd or app.config.get("ACLIENTS_MYSQL_PASSWD", None) or self.passwd
        host = host or app.config.get("ACLIENTS_MYSQL_HOST", None) or self.host
        port = port or app.config.get("ACLIENTS_MYSQL_PORT", None) or self.port
        dbname = dbname or app.config.get("ACLIENTS_MYSQL_DBNAME", None) or self.dbname
        pool_size = pool_size or app.config.get("ACLIENTS_MYSQL_POOL_SIZE", None) or self.pool_size
        message = kwargs.get("message") or app.config.get("ACLIENTS_MYSQL_MESSAGE", None) or self.message
        use_zh = kwargs.get("use_zh") or app.config.get("ACLIENTS_MYSQL_MSGZH", None) or self.use_zh

        passwd = passwd if passwd is None else str(passwd)
        self.message = verify_message(mysql_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"

        @app.listener('before_server_start')
        async def open_connection(app_, loop):
            """

            Args:

            Returns:

            """
            # engine
            # 增加autocommit = True可以解决个别情况下，提交了数据但是查询还是老的数据的问题
            self.aio_engine = await create_engine(user=username, db=dbname, host=host, port=port,
                                                  password=passwd, maxsize=pool_size, charset="utf8")

        @app.listener('after_server_stop')
        async def close_connection(app_, loop):
            """

            Args:

            Returns:

            """
            self.aio_engine.close()
            await self.aio_engine.wait_closed()

    def init_engine(self, *, username="root", passwd=None, host="127.0.0.1", port=3306, dbname=None,
                    pool_size=50, **kwargs):
        """
        mysql 实例初始化
        Args:
            host:mysql host
            port:mysql port
            dbname: database name
            username: mysql user
            passwd: mysql password
            pool_size: mysql pool size

        Returns:

        """
        username = username or self.username
        passwd = passwd or self.passwd
        host = host or self.host
        port = port or self.port
        dbname = dbname or self.dbname
        pool_size = pool_size or self.pool_size
        message = kwargs.get("message") or self.message
        use_zh = kwargs.get("use_zh") or self.use_zh

        passwd = passwd if passwd is None else str(passwd)
        self.message = verify_message(mysql_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"
        loop = asyncio.get_event_loop()

        async def open_connection():
            """

            Args:

            Returns:

            """
            # engine
            # 增加autocommit = True可以解决个别情况下，提交了数据但是查询还是老的数据的问题
            self.aio_engine = await create_engine(user=username, db=dbname, host=host, port=port,
                                                  password=passwd, maxsize=pool_size, charset="utf8")

        async def close_connection():
            """

            Args:

            Returns:

            """
            self.aio_engine.close()
            await self.aio_engine.wait_closed()

        loop.run_until_complete(open_connection())
        atexit.register(lambda: loop.run_until_complete(close_connection()))

    @staticmethod
    def _get_model_default_value(model) -> dict:
        """

        Args:
            model
        Returns:

        """
        default_values = {}
        for key, val in model.__dict__.items():
            if not key.startswith("_") and isinstance(val, InstrumentedAttribute):
                if val.default:
                    if val.default.is_callable:
                        default_values[key] = val.default.arg.__wrapped__()
                    else:
                        default_values[key] = val.default.arg
        return default_values

    @staticmethod
    def _get_model_onupdate_value(model) -> dict:
        """

        Args:
            model
        Returns:

        """
        update_values = {}
        for key, val in model.__dict__.items():
            if not key.startswith("_") and isinstance(val, InstrumentedAttribute):
                if val.onupdate and val.onupdate.is_callable:
                    update_values[key] = val.onupdate.arg.__wrapped__()
        return update_values

    async def _insert_one(self, model, insert_data: dict):
        """
        插入数据
        Args:
            model: model
            insert_data: 值类型
        Returns:
            返回插入的条数
        """
        try:
            query = insert(model).values(insert_data)
            new_values = self._get_model_default_value(model)
            new_values.update(insert_data)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with self.aio_engine.acquire() as conn:
                async with conn.begin() as trans:
                    try:
                        cursor = await conn.execute(query, new_values)
                    except IntegrityError as e:
                        await trans.rollback()
                        aelog.exception(e)
                        if "Duplicate" in str(e):
                            raise MysqlDuplicateKeyError(e)
                        else:
                            raise MysqlError(e)
                    except (MySQLError, Error) as e:
                        await trans.rollback()
                        aelog.exception(e)
                        raise MysqlError(e)
                    except Exception as e:
                        await trans.rollback()
                        aelog.exception(e)
                        raise HttpError(500, message=self.message[1][self.msg_zh], error=e)
                    else:
                        # 理论也是不应该加的，但是出现过一个连接提交后另一个连接拿不到数据的情况，而开启autocommit后就可以了，因此这里
                        # 加一条
                        await conn.execute('commit')
        return cursor.rowcount, new_values.get("id") or cursor.lastrowid

    async def _update_data(self, model, query_key: dict, or_query_key: dict, update_data: dict):
        """
        更新数据
        Args:
            model: model
            query_key: 更新的查询条件
            update_data: 值类型
            or_query_key: 或查询model的过滤条件
        Returns:
            返回更新的条数
        """
        try:
            query = update(model)
            if query_key or or_query_key:
                query = self._column_expression(model, query, query_key, or_query_key)
            query = query.values(update_data)
            new_values = self._get_model_onupdate_value(model)
            new_values.update(update_data)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with self.aio_engine.acquire() as conn:
                async with conn.begin() as trans:
                    try:
                        cursor = await conn.execute(query, new_values)
                    except IntegrityError as e:
                        await trans.rollback()
                        aelog.exception(e)
                        if "Duplicate" in str(e):
                            raise MysqlDuplicateKeyError(e)
                        else:
                            raise MysqlError(e)
                    except (MySQLError, Error) as e:
                        await trans.rollback()
                        aelog.exception(e)
                        raise MysqlError(e)
                    except Exception as e:
                        await trans.rollback()
                        aelog.exception(e)
                        raise HttpError(500, message=self.message[2][self.msg_zh], error=e)
                    else:
                        await conn.execute('commit')
        return cursor.rowcount

    async def _delete_data(self, model, query_key: dict, or_query_key: dict):
        """
        更新数据
        Args:
            model: model
            query_key: 删除的查询条件
            or_query_key: 或查询model的过滤条件
        Returns:
            返回删除的条数
        """
        try:
            query = delete(model)
            if query_key or or_query_key:
                query = self._column_expression(model, query, query_key, or_query_key)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with self.aio_engine.acquire() as conn:
                async with conn.begin() as trans:
                    try:
                        cursor = await conn.execute(query)
                    except (MySQLError, Error) as e:
                        await trans.rollback()
                        aelog.exception(e)
                        raise MysqlError(e)
                    except Exception as e:
                        await trans.rollback()
                        aelog.exception(e)
                        raise HttpError(500, message=self.message[3][self.msg_zh], error=e)
                    else:
                        await conn.execute('commit')
        return cursor.rowcount

    async def _find_one(self, model: list, query_key: dict, or_query_key: dict):
        """
        查询单条数据
        Args:
            model: 查询的model名称
            query_key: 查询model的过滤条件
            or_query_key: 或查询model的过滤条件
        Returns:
            返回匹配的数据或者None
        """
        try:
            query = select(model)
            if query_key or or_query_key:
                query = self._column_expression(model, query, query_key, or_query_key)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            try:
                async with self.aio_engine.acquire() as conn:
                    async with conn.execute(query) as cursor:
                        resp = await  cursor.fetchone()
                    await conn.execute('commit')  # 理论上不应该加这个的，但是这里默认就会启动一个事务，很奇怪
            except (MySQLError, Error) as err:
                aelog.exception("Find one data failed, {}".format(err))
                raise HttpError(400, message=self.message[4][self.msg_zh], error=err)
            else:
                return dict(resp) if resp else None

    async def _find_data(self, model: list, query_key: dict, or_query_key: dict, limit: int,
                         skip: int, order: tuple):
        """
        查询单条数据
        Args:
            model: 查询的model名称
            query_key: 查询model的过滤条件
            limit: 每页条数
            skip： 需要跳过的条数
            order: 排序条件
            or_query_key: 或查询model的过滤条件
        Returns:
            返回匹配的数据或者None
        """
        try:
            query = select(model)
            if query_key or or_query_key:
                query = self._column_expression(model, query, query_key, or_query_key)
            if order:
                query = query.order_by(desc(order[0])) if order[1] == 1 else query.order_by(order[0])
            if limit:
                query = query.limit(limit)
            if skip:
                query = query.offset(skip)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            try:
                async with self.aio_engine.acquire() as conn:
                    async with conn.execute(query) as cursor:
                        resp = await  cursor.fetchall()
                    await conn.execute('commit')
            except (MySQLError, Error) as err:
                aelog.exception("Find data failed, {}".format(err))
                raise HttpError(400, message=self.message[5][self.msg_zh], error=err)
            else:
                return [dict(val) for val in resp] if resp else []

    async def _find_count(self, model, query_key: dict, or_query_key: dict):
        """
        查询单条数据
        Args:
            model: 查询的model名称
            query_key: 查询model的过滤条件
            or_query_key: 或查询model的过滤条件
        Returns:
            返回条数
        """
        try:
            query = select([func.count().label("count")]).select_from(model)
            if query_key or or_query_key:
                query = self._column_expression(model, query, query_key, or_query_key)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            try:
                async with self.aio_engine.acquire() as conn:
                    async with conn.execute(query) as cursor:
                        resp = await  cursor.fetchone()
                    await conn.execute('commit')
            except (MySQLError, Error) as err:
                aelog.exception("Find data failed, {}".format(err))
                raise HttpError(400, message=self.message[5][self.msg_zh], error=err)
            else:
                return resp.count

    @staticmethod
    def _column_expression(model: list, query, query_key: dict, or_query_key: dict):
        """
        查询单条数据
        Args:
            model: 查询的model名称
            query: 查询的query基本expression
            query_key: 查询model的过滤条件
            or_query_key: 或查询model的过滤条件
        Returns:
            返回匹配的数据或者None
        """
        model = model if isinstance(model, MutableSequence) else [model]
        query_key = query_key if isinstance(query_key, MutableMapping) else {}
        or_query_key = or_query_key if isinstance(or_query_key, MutableMapping) else {}

        maps = {
            "eq": lambda column_name, column_val: query.where(column_name == column_val),
            "ne": lambda column_name, column_val: query.where(column_name != column_val),
            "gt": lambda column_name, column_val: query.where(column_name > column_val),
            "gte": lambda column_name, column_val: query.where(column_name >= column_val),
            "lt": lambda column_name, column_val: query.where(column_name < column_val),
            "lte": lambda column_name, column_val: query.where(column_name <= column_val),
            "in": lambda column_name, column_val: query.where(column_name.in_(column_val)),
            "nin": lambda column_name, column_val: query.where(column_name.notin_(column_val)),
            "like": lambda column_name, column_val: query.where(column_name.like(column_val)),
            "ilike": lambda column_name, column_val: query.where(column_name.ilike(column_val)),
            "between": lambda column_name, column_val: query.where(
                column_name.between(column_val[0], column_val[1]))}
        or_maps = {
            "eq": lambda column_name, column_val: column_name == column_val,
            "ne": lambda column_name, column_val: column_name != column_val,
            "gt": lambda column_name, column_val: column_name > column_val,
            "gte": lambda column_name, column_val: column_name >= column_val,
            "lt": lambda column_name, column_val: column_name < column_val,
            "lte": lambda column_name, column_val: column_name <= column_val,
            "in": lambda column_name, column_val: column_name.in_(column_val),
            "nin": lambda column_name, column_val: column_name.notin_(column_val),
            "like": lambda column_name, column_val: column_name.like(column_val),
            "ilike": lambda column_name, column_val: column_name.ilike(column_val),
            "between": lambda column_name, column_val: column_name.between(column_val[0], column_val[1])
        }

        def or_query(column_val: MutableSequence):
            """
            组装or查询表达式
            Args:

            Returns:

            """
            return query.where(or_(*column_val))

        # 如果出现[model1, model2]则只考虑第一个，因为如果多表查询，则必须在query_key中指定清楚，这里只处理大多数情况
        model = model[0] if not isinstance(model[0], InstrumentedAttribute) else getattr(model[0], "class_")
        for field_name, val in query_key.items():
            field_name = getattr(model, field_name) if not isinstance(field_name, InstrumentedAttribute) else field_name
            # 因为判断相等的查询比较多，因此默认就是==
            if not isinstance(val, MutableMapping):
                query = maps["eq"](field_name, val)
            else:
                # 其他情况则需要指定是什么查询，大于、小于或者是like等
                # 可能会出现多个查询的情况，比如{"key": {"gt": 3, "lt": 9}}
                for operate, value in val.items():
                    if operate in maps:
                        query = maps[operate](field_name, value)
        # or 查询 {"key": {"gt": 3, "lt": 9}}
        for field_name, or_value in or_query_key.items():
            field_name = getattr(model, field_name) if not isinstance(field_name, InstrumentedAttribute) else field_name
            or_args = []
            for operate, sub_or_value in or_value.items():
                if operate in or_maps:
                    if not isinstance(sub_or_value, MutableSequence):
                        or_args.append(or_maps[operate](field_name, sub_or_value))
                    else:
                        for val in sub_or_value:
                            or_args.append(or_maps[operate](field_name, val))
            else:
                query = or_query(or_args)
        return query

    async def execute(self, query):
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        async with self.aio_engine.acquire() as conn:
            async with conn.begin() as trans:
                try:
                    cursor = await conn.execute(query)
                except IntegrityError as e:
                    await trans.rollback()
                    aelog.exception(e)
                    if "Duplicate" in str(e):
                        raise MysqlDuplicateKeyError(e)
                    else:
                        raise MysqlError(e)
                except (MySQLError, Error) as e:
                    await trans.rollback()
                    aelog.exception(e)
                    raise MysqlError(e)
                except Exception as e:
                    await trans.rollback()
                    aelog.exception(e)
                    raise HttpError(500, message=self.message[6][self.msg_zh], error=e)
                else:
                    await conn.execute('commit')
        return cursor

    async def query(self, query):
        """
        查询数据，用于复杂的查询
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        try:
            async with self.aio_engine.acquire() as conn:
                async with conn.execute(query) as cursor:
                    resp = await  cursor.fetchall()
                await conn.execute('commit')
        except (MySQLError, Error) as err:
            aelog.exception("Find data failed, {}".format(err))
            raise HttpError(400, message=self.message[5][self.msg_zh], error=err)
        else:
            return [dict(val) for val in resp] if resp else None

    async def insert_one(self, model, *, insert_data: dict):
        """
        插入数据
        Args:
            model: model
            insert_data: 值类型
        Returns:
            返回插入的条数
        """
        return await self._insert_one(model, insert_data)

    async def find_one(self, model: DeclarativeMeta or list, *, query_key: dict = None, or_query_key: dict = None):
        """
        查询单条数据
        Args:
            model: 查询的model名称
            query_key: 查询model的过滤条件
            or_query_key: 或查询model的过滤条件
        Returns:
            返回匹配的数据或者None
        """
        model = model if isinstance(model, MutableSequence) else [model]
        return await self._find_one(model, query_key, or_query_key)

    async def find_data(self, model: DeclarativeMeta or list, *, query_key: dict = None, or_query_key: dict = None,
                        limit: int = 0, page: int = 1, order: tuple = None):
        """
        插入数据
        Args:
            model: model
            query_key: 查询表的过滤条件, {"key": {"gt": 3, "lt": 9}}
            or_query_key: 或查询model的过滤条件,{"key": {"gt": 3, "lt": 9}},{"key": {"eq": [3, 8]}}
            limit: 限制返回的表的条数
            page: 从查询结果中调过指定数量的行
            order: 排序条件
        Returns:

        """
        if order and not isinstance(order, (list, tuple)):
            raise FuncArgsError("order must be tuple or list!")
        limit = int(limit)
        skip = (int(page) - 1) * limit
        model = model if isinstance(model, MutableSequence) else [model]
        return await self._find_data(model, query_key, or_query_key, limit=limit, skip=skip, order=order)

    async def find_count(self, model, *, query_key: dict = None, or_query_key: dict = None):
        """
        查询单条数据
        Args:
            model: 查询的model名称
            query_key: 查询model的过滤条件
            or_query_key: 或查询model的过滤条件
        Returns:
            返回总条数
        """
        return await self._find_count(model, query_key, or_query_key)

    async def update_data(self, model, *, query_key: dict, or_query_key: dict = None, update_data: dict):
        """
        更新数据
        Args:
            model: model
            query_key: 更新的查询条件
            or_query_key: 或查询model的过滤条件
            update_data: 值类型
        Returns:
            返回更新的条数
        """
        return await self._update_data(model, query_key, or_query_key, update_data)

    async def delete_data(self, model, *, query_key: dict, or_query_key: dict = None):
        """
        更新数据
        Args:
            model: model
            query_key: 删除的查询条件, 必须有query_key，不允许删除整张表
            or_query_key: 或查询model的过滤条件
        Returns:
            返回删除的条数
        """
        if not query_key:
            raise FuncArgsError("query_key must be provide!")
        return await self._delete_data(model, query_key, or_query_key)
