#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午4:58
"""
import asyncio
import atexit
from math import ceil
from typing import (Dict, List, MutableMapping, Tuple)

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


class BaseQuery(object):
    """
    查询
    """

    def __init__(self, ):
        """
            查询
        Args:

        """
        self._whereclause: List = []
        self._order_by: List = []
        self._group_by: List = []
        self._having: List = []
        self._distinct: List = []
        self._columns: List = None
        self._union: List = None
        self._union_all: List = None
        self._with_hint: List = None
        self._bind_values: Dict = None

    def where(self, *whereclause):
        """return a new select() construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        """

        self._whereclause.extend(whereclause)
        return self

    def order_by(self, *clauses):
        """return a new selectable with the given list of ORDER BY
        criterion applied.

        The criterion will be appended to any pre-existing ORDER BY
        criterion.

        """

        self._order_by.extend(clauses)
        return self

    def group_by(self, *clauses):
        """return a new selectable with the given list of GROUP BY
        criterion applied.

        The criterion will be appended to any pre-existing GROUP BY
        criterion.

        """

        self._group_by.extend(clauses)
        return self

    def having(self, *having):
        """return a new select() construct with the given expression added to
        its HAVING clause, joined to the existing clause via AND, if any.

        """
        self._having.extend(having)

    def distinct(self, *expr):
        r"""Return a new select() construct which will apply DISTINCT to its
        columns clause.

        :param \*expr: optional column expressions.  When present,
         the PostgreSQL dialect will render a ``DISTINCT ON (<expressions>>)``
         construct.

        """
        self._distinct.extend(expr)
        return self

    def columns(self, columns):
        r"""Return a new :func:`.select` construct with its columns
        clause replaced with the given columns.

        This method is exactly equivalent to as if the original
        :func:`.select` had been called with the given columns
        clause.   I.e. a statement::

            s = select([table1.c.a, table1.c.b])
            s = s.with_only_columns([table1.c.b])

        This means that FROM clauses which are only derived
        from the column list will be discarded if the new column
        list no longer contains that FROM::

        """
        self._columns = columns
        return self

    def union(self, other, **kwargs):
        """return a SQL UNION of this select() construct against the given
        selectable."""

        self._union = [other, kwargs]
        return self

    def union_all(self, other, **kwargs):
        """return a SQL UNION ALL of this select() construct against the given
        selectable.

        """
        self._union_all = [other, kwargs]
        return self

    def with_hint(self, selectable, text_, dialect_name='*'):
        r"""Add an indexing or other executional context hint for the given
        selectable to this :class:`.Select`.

        The text of the hint is rendered in the appropriate
        location for the database backend in use, relative
        to the given :class:`.Table` or :class:`.Alias` passed as the
        ``selectable`` argument. The dialect implementation
        typically uses Python string substitution syntax
        with the token ``%(name)s`` to render the name of
        the table or alias. E.g. when using Oracle, the
        following::

            select([mytable]).\
                with_hint(mytable, "index(%(name)s ix_mytable)")

        Would render SQL as::

            select /*+ index(mytable ix_mytable) */ ... from mytable

        The ``dialect_name`` option will limit the rendering of a particular
        hint to a particular backend. Such as, to add hints for both Oracle
        and Sybase simultaneously::

            select([mytable]).\
                with_hint(mytable, "index(%(name)s ix_mytable)", 'oracle').\
                with_hint(mytable, "WITH INDEX ix_mytable", 'sybase')

        .. seealso::

            :meth:`.Select.with_statement_hint`

        """
        self._with_hint = [selectable, text_, dialect_name]
        return self

    def values(self, **kwargs):
        r"""specify a fixed VALUES clause for an SET clause for an UPDATE."""
        self._bind_values = kwargs
        return self


# noinspection PyProtectedMember
class Pagination(object):
    """Internal helper class returned by :meth:`BaseQuery.paginate`.  You
    can also construct it from any other SQLAlchemy query object if you are
    working with other libraries.  Additionally it is possible to pass `None`
    as query object in which case the :meth:`prev` and :meth:`next` will
    no longer work.
    """

    def __init__(self, db_client, select_query, page, per_page, total, items):
        #: the unlimited query object that was used to create this
        #: aiomysqlclient object.
        self._db_client = db_client
        #: select query
        self._select_query = select_query
        #: the current page number (1 indexed)
        self.page = page
        #: the number of items to be displayed on a page.
        self.per_page = per_page
        #: the total number of items matching the query
        self.total = total
        #: the items for the current page
        self.items = items

    @property
    def pages(self):
        """The total number of pages"""
        if self.per_page == 0:
            pages = 0
        else:
            pages = int(ceil(self.total / float(self.per_page)))
        return pages

    async def prev(self, ):
        """Returns a :class:`Pagination` object for the previous page."""
        self._select_query.limit(self.per_page)
        self._select_query.offset((self.page - 1 - 1) * self.per_page)
        return await self._db_client._find_data(self._select_query)

    @property
    def prev_num(self):
        """Number of the previous page."""
        if not self.has_prev:
            return None
        return self.page - 1

    @property
    def has_prev(self):
        """True if a previous page exists"""
        return self.page > 1

    async def next(self, ):
        """Returns a :class:`Pagination` object for the next page."""
        self._select_query.limit(self.per_page)
        self._select_query.offset(self.page - 1 + 1 * self.per_page)
        return await self._db_client._find_data(self._select_query)

    @property
    def has_next(self):
        """True if a next page exists."""
        return self.page < self.pages

    @property
    def next_num(self):
        """Number of the next page"""
        if not self.has_next:
            return None
        return self.page + 1


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
        self.max_per_page = kwargs.get("max_per_page", None)
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
        self.max_per_page = kwargs.get("max_per_page", None) or self.max_per_page
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
        self.max_per_page = kwargs.get("max_per_page", None) or self.max_per_page
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

    @property
    def query(self, ) -> BaseQuery:
        """

        Args:

        Returns:

        """
        return BaseQuery()

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

    async def _query_execute(self, query, params: Dict = None) -> ResultProxy:
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
                cursor = await conn.execute(query, params or {})
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

    async def _update_data(self, model, query: BaseQuery, update_data: Dict or List) -> int:
        """
        更新数据

        eg: where(User.c.id == bindparam("id")).values({"name": bindparam("name")})
         await conn.execute(sql, [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}]
        Args:
            model: sqlalchemy中的model或者table
            query: 更新的BaseQuery查询类
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

            select_query = update(model)
            for one_clause in query._whereclause:
                select_query.where(one_clause)
            else:
                select_query.values(update_data_ if query._bind_values is None else query._bind_values)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with await self._execute(select_query, update_data_, 2) as cursor:
                return cursor.rowcount

    async def _delete_data(self, model, query: BaseQuery) -> int:
        """
        更新数据
        Args:
            model: sqlalchemy中的model或者table
            query: 删除的BaseQuery查询类
        Returns:
            返回删除的条数
        """
        try:
            select_query = delete(model)
            for one_clause in query._whereclause:
                select_query.where(one_clause)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            rowcount = 0
            async with self.aio_engine.acquire() as conn:
                async with conn.begin() as trans:
                    try:
                        async with conn.execute(select_query) as cursor:
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

    async def _find_one(self, model: DeclarativeMeta, query: BaseQuery) -> RowProxy or None:
        """
        查询单条数据
        Args:
            model: sqlalchemy中的model或者table
            query: BaseQuery查询类
        Returns:
            返回匹配的数据或者None
        """
        try:
            query_ = select(model)
            if query._with_hint:
                query_.with_hint(*query._with_hint)
            for one_clause in query._whereclause:
                query_.append_whereclause(one_clause)
            if query._order_by:
                query_.order_by(*query._order_by)
            if query._distinct:
                query_.distinct(*query._distinct)
            if query._columns:
                query_.with_only_columns(query._columns)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            cursor = await self._query_execute(query_)
            return await cursor.first() if cursor.returns_rows else None

    async def _find_data(self, select_query) -> List[RowProxy] or []:
        """
        查询单条数据
        Args:
            select_query: sqlalchemy中的select query
        Returns:
            返回匹配的数据或者None
        """
        cursor = await self._query_execute(select_query)
        return await cursor.fetchall() if cursor.returns_rows else []

    async def _find_count(self, select_query) -> RowProxy:
        """
        查询单条数据
        Args:
            select_query: sqlalchemy中的select query
        Returns:
            返回条数
        """
        cursor = await self._query_execute(select_query)
        return await cursor.first()

    @staticmethod
    def gen_query(select_query, query: BaseQuery, *, limit_clause: int = None, offset_clause: int = None):
        """
        查询单条数据
        Args:
            select_query: sqlalchemy中的select query
            query: BaseQuery查询类
            limit_clause: 每页数量
            offset_clause: 跳转数量
        Returns:
            返回条数
        """
        if query._with_hint:
            select_query.with_hint(*query._with_hint)
        if query._whereclause:
            for one_clause in query._whereclause:
                select_query.append_whereclause(one_clause)
        if query._order_by:
            select_query.order_by(*query._order_by)
        if query._group_by:
            select_query.group_by(*query._group_by)
            for one_clause in query._having:
                select_query.append_having(one_clause)
        if query._distinct:
            select_query.distinct(*query._distinct)
        if query._columns:
            select_query.with_only_columns(query._columns)
        if limit_clause is not None:
            select_query.limit(limit_clause)
        if offset_clause is not None:
            select_query.offset(offset_clause)

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

    async def query_execute(self, query, params: Dict = None, size=None, cursor_close=True
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
            List[RowProxy] or RowProxy or None
        """
        params = dict(params) if isinstance(params, MutableMapping) else {}
        cursor = await self._query_execute(query, params)

        if size is None:
            resp = await cursor.fetchall() if cursor.returns_rows else []
        elif size == 1:
            resp = await cursor.fetchone() if cursor.returns_rows else None
        else:
            resp = await cursor.fetchmany(size) if cursor.returns_rows else []

        if cursor_close is True:
            await cursor.close()

        return resp

    async def find_one(self, model: DeclarativeMeta, *, query: BaseQuery = None) -> RowProxy or None:
        """
        查询单条数据
        Args:
            model: sqlalchemy中的model或者table
            query: BaseQuery查询类
        Returns:
            返回匹配的数据或者None
        """
        if not isinstance(model, DeclarativeMeta):
            raise FuncArgsError("model type error!")
        query = BaseQuery() if query is None else query
        return await self._find_one(model, query)

    async def find_many(self, model: DeclarativeMeta, *, query: BaseQuery = None,
                        page: int = 1, per_page: int = 20, primary_order=True) -> Pagination:
        """Returns ``per_page`` items from page ``page``.

        If ``page`` or ``per_page`` are ``None``, they will be retrieved from
        the request query. If ``max_per_page`` is specified, ``per_page`` will
        be limited to that value. If there is no request or they aren't in the
        query, they default to 1 and 20 respectively.

        * No items are found and ``page`` is not 1.
        * ``page`` is less than 1, or ``per_page`` is negative.
        * ``page`` or ``per_page`` are not ints.
        * primary_order: 默认启用主键ID排序的功能，在大数据查询时可以关闭此功能，在90%数据量不大的情况下可以加快分页的速度

        When ``error_out`` is ``False``, ``page`` and ``per_page`` default to
        1 and 20 respectively.

        Returns a :class:`Pagination` object.

        目前是改造如果limit传递为0，则返回所有的数据，这样业务代码中就不用更改了
        """

        if self.max_per_page is not None:
            per_page = min(per_page, self.max_per_page)

        if page < 1:
            page = 1

        if per_page < 0:
            per_page = 20

        try:
            select_query = select(model)
            # 如果per_page为0,则证明要获取所有的数据，否则还是通常的逻辑
            if per_page != 0:
                self.gen_query(select_query, query, limit_clause=per_page, offset_clause=(page - 1) * per_page)
                # 如果分页获取的时候没有进行排序,并且model中有id字段,则增加用id字段的升序排序
                # 前提是默认id是主键,因为不排序会有混乱数据,所以从中间件直接解决,业务层不需要关心了
                # 如果业务层有排序了，则此处不再提供排序功能
                # 如果遇到大数据量的分页查询问题时，建议关闭此处，然后再基于已有的索引分页
                if primary_order is True and getattr(model, "id", None) is not None:
                    select_query.order_by(getattr(model, "id").asc())
            else:
                self.gen_query(select_query, query)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            items = await self._find_data(select_query)

        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if page == 1 and len(items) < per_page:
            total = len(items)
        else:
            total_result = await self.find_count(model, query=query)
            total = total_result.count

        return Pagination(self, select_query, page, per_page, total, items)

    async def find_all(self, model: DeclarativeMeta, *, query: BaseQuery = None) -> List[RowProxy] or []:
        """
        插入数据
        Args:
            model: sqlalchemy中的model或者table
            query: BaseQuery查询类
        Returns:

        """
        if not isinstance(model, DeclarativeMeta):
            raise FuncArgsError("model type error!")
        query = BaseQuery() if query is None else query

        try:
            select_query = select(model)
            self.gen_query(select_query, query)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            return await self._find_data(select_query)

    async def find_count(self, model: DeclarativeMeta, *, query: BaseQuery = None) -> RowProxy:
        """
        查询单条数据
        Args:
            model: sqlalchemy中的model或者table
            query: BaseQuery查询类
        Returns:
            返回总条数
        """
        if not isinstance(model, DeclarativeMeta):
            raise FuncArgsError("model type error!")
        query = BaseQuery() if query is None else query

        try:
            select_query = select([func.count().label("count")]).select_from(model)
            self.gen_query(select_query, query)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            return await self._find_count(select_query)

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

    async def update_data(self, model, *, query: BaseQuery, update_data: Dict or List) -> int:
        """
        更新数据

        eg: where(User.c.id == bindparam("id")).values({"name": bindparam("name")})
         await conn.execute(sql, [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}]
        Args:
            model: sqlalchemy中的model或者table
            query: 更新的BaseQuery查询类
            update_data: 值类型,可以单个条件更新或者多个数据多个条件更新
        Returns:
            返回更新的条数
        """
        query = query if isinstance(query, BaseQuery) else BaseQuery()
        return await self._update_data(model, query, update_data)

    async def delete_data(self, model, *, query: BaseQuery) -> int:
        """
        更新数据
        Args:
            model: sqlalchemy中的model或者table
            query: 删除的BaseQuery查询类, 必须有query，不允许删除整张表
        Returns:
            返回删除的条数
        """
        if not query:
            raise FuncArgsError("query_key must be provide!")
        query = query if isinstance(query, BaseQuery) else BaseQuery()
        return await self._delete_data(model, query)
