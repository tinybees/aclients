#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午4:58
"""
import asyncio
import atexit
from typing import (Dict, List, MutableMapping, MutableSequence, Tuple)

import aelog
from aiomysql.sa import create_engine
from aiomysql.sa.exc import Error
from aiomysql.sa.result import ResultProxy, RowProxy
from pymysql.err import IntegrityError, MySQLError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import (Select, all_, and_, any_, asc, bindparam, case, cast, column, delete, desc, distinct,
                            except_, except_all, exists, extract, false, func, funcfilter, insert, intersect,
                            intersect_all, join, label, not_, null, nullsfirst, nullslast, or_, outerjoin, over, select,
                            table, text, true, tuple_, type_coerce, union, union_all, update, within_group)

from .err_msg import mysql_msg
from .exceptions import FuncArgsError, HttpError, MysqlDuplicateKeyError, MysqlError, QueryArgsError
from .utils import verify_message

__all__ = ("AIOMysqlClient", "all_", "any_", "and_", "or_", "bindparam", "select", "text", "table", "column",
           "over", "within_group", "label", "case", "cast", "extract", "tuple_", "except_", "except_all", "intersect",
           "intersect_all", "union", "union_all", "exists", "nullsfirst", "nullslast", "asc", "desc", "distinct",
           "type_coerce", "true", "false", "null", "join", "outerjoin", "funcfilter", "func", "not_")


class AIOMysqlClient(object):
    """
    MySQL异步操作指南
    """

    def __init__(self, app=None, *, username="root", passwd=None, host="127.0.0.1", port=3306, dbname=None,
                 pool_size=50, **kwargs):
        """
        mysql 非阻塞工具类
        Args:
            app: app应用
            host:mysql host
            port:mysql port
            dbname: database name
            username: mysql user
            passwd: mysql password
            pool_size: mysql pool size
        """
        self.app = app
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
        self.app = app

        @app.listener('before_server_start')
        async def open_connection(app_, loop):
            """

            Args:

            Returns:

            """
            # engine
            self.aio_engine = await create_engine(user=username, db=dbname, host=host, port=port,
                                                  password=passwd, maxsize=pool_size, charset="utf8mb4")

        @app.listener('after_server_stop')
        async def close_connection(app_, loop):
            """

            Args:

            Returns:

            """
            if self.aio_engine:
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
            self.aio_engine = await create_engine(user=username, db=dbname, host=host, port=port,
                                                  password=passwd, maxsize=pool_size, charset="utf8")

        async def close_connection():
            """

            Args:

            Returns:

            """
            if self.aio_engine:
                self.aio_engine.close()
                await self.aio_engine.wait_closed()

        loop.run_until_complete(open_connection())
        atexit.register(lambda: loop.run_until_complete(close_connection()))

    @staticmethod
    def _get_model_default_value(model) -> Dict:
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
    def _get_model_onupdate_value(model) -> Dict:
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

    async def _execute(self, query, params: List or Dict, msg_code: int) -> ResultProxy:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: 执行的参数值,可以是单个对象的字典也可以是多个对象的列表
            msg_code: 消息提示编码
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        async with self.aio_engine.acquire() as conn:
            async with conn.begin() as trans:
                try:
                    cursor = await conn.execute(query, params)
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
                    raise HttpError(400, message=self.message[msg_code][self.msg_zh])
                else:
                    await trans.commit()
        return cursor

    async def _query_execute(self, query, params: Dict) -> ResultProxy:
        """
        查询数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: 执行的参数值,
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        async with self.aio_engine.acquire() as conn:
            try:
                cursor = await conn.execute(query, params)
            except (MySQLError, Error) as e:
                aelog.exception("Find data failed, {}".format(e))
                raise HttpError(400, message=self.message[4][self.msg_zh])
            except Exception as e:
                aelog.exception(e)
                raise HttpError(400, message=self.message[4][self.msg_zh])

        return cursor

    async def _insert_one(self, model, insert_data: Dict) -> Tuple[int, str]:
        """
        插入数据
        Args:
            model: sqlalchemy中的model或者table
            insert_data: 值类型
        Returns:
            (插入的条数,插入的ID)
        """
        try:
            insert_data_ = self._get_model_default_value(model)
            insert_data_.update(insert_data)
            query = insert(model).values(insert_data_)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with self._execute(query, insert_data_, 1) as cursor:
                return cursor.rowcount, insert_data_.get("id") or cursor.lastrowid

    async def _insert_from_select(self, model, column_names: List, select_query: Select) -> Tuple[int, str]:
        """
        查询并且插入数据, ``INSERT...FROM SELECT`` statement.

        e.g.::

            sel = select([table1.c.a, table1.c.b]).where(table1.c.c > 5)
            ins = table2.insert().from_select(['a', 'b'], sel)
        Args:
            model: sqlalchemy中的model或者table
            column_names: 字符串列名列表或者Column类列名列表
            select_query: select表达式
        Returns:
            (插入的条数,插入的ID)
        """
        try:
            query = insert(model).from_select(column_names, select_query)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with await self._execute(query, {}, 1) as cursor:
                return cursor.rowcount, cursor.lastrowid

    async def _insert_many(self, model, insert_data: List[Dict]) -> int:
        """
        插入多条数据

        eg: User.insert().values([{"name": "test1"}, {"name": "test2"}]
        Args:
            model: sqlalchemy中的model或者table
            insert_data: 值类型
        Returns:
            插入的条数
        """
        try:
            insert_data_ = self._get_model_default_value(model)
            insert_data_ = [{**insert_data_, **one_data} for one_data in insert_data]
            query = insert(model).values(insert_data_)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with self._execute(query, insert_data_, 1) as cursor:
                return cursor.rowcount

    async def _update_data(self, model, query_key: List, update_data: Dict or List) -> int:
        """
        更新数据

        eg: where(User.c.id == bindparam("id")).values({"name": bindparam("name")})
         await conn.execute(sql, [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}]
        Args:
            model: sqlalchemy中的model或者table
            query_key: 更新的查询条件
            update_data: 值类型
        Returns:
            返回更新的条数
        """
        try:
            update_data_ = self._get_model_onupdate_value(model)
            if isinstance(update_data, MutableMapping):
                update_data_ = {**update_data_, **update_data}
            else:
                update_data_ = [{**update_data_, **one_data} for one_data in update_data]
            query = update(model).where(*query_key).values(update_data_)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with await self._execute(query, update_data_, 2) as cursor:
                return cursor.rowcount

    async def _delete_data(self, model, query_key: List) -> int:
        """
        更新数据
        Args:
            model: sqlalchemy中的model或者table
            query_key: 删除的查询条件
        Returns:
            返回删除的条数
        """
        try:
            query = delete(model).where(*query_key)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            rowcount = 0
            async with self.aio_engine.acquire() as conn:
                async with conn.begin() as trans:
                    try:
                        async with conn.execute(query) as cursor:
                            rowcount = cursor.rowcount
                    except (MySQLError, Error) as e:
                        await trans.rollback()
                        aelog.exception(e)
                        raise MysqlError(e)
                    except Exception as e:
                        await trans.rollback()
                        aelog.exception(e)
                        raise HttpError(400, message=self.message[3][self.msg_zh])
                    else:
                        await trans.commit()
            return rowcount

    async def _find_one(self, model: List, query_key: List, ):
        """
        查询单条数据
        Args:
            model: sqlalchemy中的model或者table
            query_key: 查询model的过滤条件
        Returns:
            返回匹配的数据或者None
        """
        try:
            query = select(model).where
            # if query_key:
            #     query = self._column_expression(model, query, query_key, )
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with self._query_execute(query) as cursor:
                resp = await cursor.fetchone()
            return dict(resp) if resp else None

    async def _find_data(self, model: List, query_key: dict, limit: int,
                         skip: int, ):
        """
        查询单条数据
        Args:
            model: sqlalchemy中的model或者table
            query_key: 查询model的过滤条件
            limit: 每页条数
            skip： 需要跳过的条数
            order: 排序条件
        Returns:
            返回匹配的数据或者None
        """
        try:
            query = select(model)
            # if query_key or or_query_key:
            #     query = self._column_expression(model, query, query_key, or_query_key)
            # if order:
            #     query = query.order_by(desc(order[0])) if order[1] == 1 else query.order_by(order[0])
            # else:
            #     model_ = model[0] if isinstance(model, MutableSequence) else model
            #     if getattr(model_, "id", None) is not None:
            #         query = query.order_by(asc("id"))
            if limit:
                query = query.limit(limit)
            if skip:
                query = query.offset(skip)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with self._query_execute(query) as cursor:
                resp = await cursor.fetchall()
            return [dict(val) for val in resp] if resp else []

    async def _find_count(self, model, query_key: dict):
        """
        查询单条数据
        Args:
            model: sqlalchemy中的model或者table
            query_key: 查询model的过滤条件
        Returns:
            返回条数
        """
        try:
            query = select([func.count().label("count")]).select_from(model)
            # if query_key:
            #     query = self._column_expression(model, query, query_key)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with self._query_execute(query) as cursor:
                resp = await cursor.fetchone()
            return resp.count

    async def execute(self, query, params: Dict) -> int:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: 执行的参数值,可以是单个对象的字典也可以是多个对象的列表
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        params = dict(params) if isinstance(params, MutableMapping) else {}
        async with await self._execute(query, params, 6) as cursor:
            return cursor.rowcount

    async def query(self, query, params: Dict = None, size=None, cursor_close=True
                    ) -> List[RowProxy] or RowProxy or None:
        """
        查询数据，用于复杂的查询
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            size: 查询数据大小, 默认返回所有
            params: SQL表达式中的参数
            size: 查询数据大小, 默认返回所有
            cursor_close: 是否关闭游标，默认关闭，如果多次读取可以改为false，后面关闭的行为交给sqlalchemy处理

        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        params = dict(params) if isinstance(params, MutableMapping) else {}
        cursor = await self._query_execute(query, params)

        if size is None:
            resp = await cursor.fetchall()
        elif size == 1:
            resp = await cursor.fetchone()
        else:
            resp = await cursor.fetchmany(size)

        if cursor_close is True:
            await cursor.close()

        return resp

    async def find_one(self, model: DeclarativeMeta or List, *, query_key: Dict = None, ):
        """
        查询单条数据
        Args:
            model: sqlalchemy中的model或者table
            query_key: 查询model的过滤条件
        Returns:
            返回匹配的数据或者None
        """
        model = model if isinstance(model, MutableSequence) else [model]
        return await self._find_one(model, query_key, )

    async def find_data(self, model: DeclarativeMeta or List, *, query_key: Dict = None,
                        limit: int = 0, page: int = 1, order: tuple = None):
        """
        插入数据
        Args:
            model: sqlalchemy中的model或者table
            query_key: 查询表的过滤条件, {"key": {"gt": 3, "lt": 9}}
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
        return await self._find_data(model, query_key, limit=limit, skip=skip, order=order)

    async def find_count(self, model, *, query_key: Dict = None):
        """
        查询单条数据
        Args:
            model: sqlalchemy中的model或者table
            query_key: 查询model的过滤条件
        Returns:
            返回总条数
        """
        return await self._find_count(model, query_key)

    async def insert_one(self, model, *, insert_data: Dict) -> Tuple[int, str]:
        """
        插入数据
        Args:
            model: sqlalchemy中的model或者table
            insert_data: 插入的单条dict类型数据
        Returns:
            (插入的条数,插入的ID)
        """
        return await self._insert_one(model, insert_data)

    async def insert_many(self, model, *, insert_data: List[Dict]) -> int:
        """
        插入多条数据

        eg: User.insert().values([{"name": "test1"}, {"name": "test2"}]
        Args:
            model: sqlalchemy中的model或者table
            insert_data: 插入的多条[dict]数据
        Returns:
            插入的条数
        """
        return await self._insert_many(model, insert_data)

    async def insert_from_select(self, model, column_names: List, select_query: Select) -> Tuple[int, str]:
        """
        插入数据
        Args:
            model: sqlalchemy中的model或者table
            column_names: 字符串列名列表或者Column类列名列表
            select_query: select表达式
        Returns:
            (插入的条数,插入的ID)
        """
        if not column_names:
            raise FuncArgsError("column_names must be provide!")
        return await self._insert_from_select(model, column_names, select_query)

    async def update_data(self, model, *, query_key: List, update_data: Dict or List) -> int:
        """
        更新数据

        eg: where(User.c.id == bindparam("id")).values({"name": bindparam("name")})
         await conn.execute(sql, [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}]
        Args:
            model: sqlalchemy中的model或者table
            query_key: 更新的查询条件
            update_data: 值类型,可以单个条件更新或者多个数据多个条件更新
        Returns:
            返回更新的条数
        """
        return await self._update_data(model, query_key, update_data)

    async def delete_data(self, model, *, query_key: List) -> int:
        """
        更新数据
        Args:
            model: sqlalchemy中的model或者table
            query_key: 删除的查询条件, 必须有query_key，不允许删除整张表
        Returns:
            返回删除的条数
        """
        if not query_key:
            raise FuncArgsError("query_key must be provide!")
        return await self._delete_data(model, query_key)
