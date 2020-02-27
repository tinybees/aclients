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
from typing import (Dict, List, MutableMapping, Optional, Tuple, Union)

import aelog
import sqlalchemy as sa
from aiomysql.sa import create_engine
from aiomysql.sa.exc import Error
from aiomysql.sa.result import ResultProxy, RowProxy
from pymysql.err import IntegrityError, MySQLError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import (Select, all_, and_, any_, asc, bindparam, case, cast, column, delete, desc, distinct,
                            except_, except_all, exists, extract, false, func, funcfilter, insert, intersect,
                            intersect_all, join, label, not_, null, nullsfirst, nullslast, or_, outerjoin, over, select,
                            table, text, true, tuple_, type_coerce, union, union_all, update, within_group)

from .err_msg import mysql_msg
from .exceptions import ConfigError, FuncArgsError, HttpError, MysqlDuplicateKeyError, MysqlError, QueryArgsError
from .utils import gen_class_name, verify_message

__all__ = ("AIOMysqlClient", "all_", "any_", "and_", "or_", "bindparam", "select", "text", "table", "column",
           "over", "within_group", "label", "case", "cast", "extract", "tuple_", "except_", "except_all", "intersect",
           "intersect_all", "union", "union_all", "exists", "nullsfirst", "nullslast", "asc", "desc", "distinct",
           "type_coerce", "true", "false", "null", "join", "outerjoin", "funcfilter", "func", "not_", "Select",
           "update", "delete")


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
        self._columns: List = []
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

        :param expr: optional column expressions.  When present,
         the PostgreSQL dialect will render a ``DISTINCT ON (<expressions>>)``
         construct.

        """
        self._distinct.extend(expr)
        return self

    def columns(self, *columns):
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
        self._columns.extend(columns)
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

    def __init__(self, db_client, select_query: Select, page: int, per_page: int, total: int, items: List[Dict]):
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
    def pages(self) -> int:
        """The total number of pages"""
        if self.per_page == 0:
            pages = 0
        else:
            pages = int(ceil(self.total / float(self.per_page)))
        return pages

    async def prev(self, ) -> List[RowProxy]:
        """Returns a :class:`Pagination` object for the previous page."""
        self._select_query.limit(self.per_page)
        self._select_query.offset((self.page - 1 - 1) * self.per_page)
        return await self._db_client._find_data(self._select_query)

    @property
    def prev_num(self) -> int:
        """Number of the previous page."""
        if not self.has_prev:
            return None
        return self.page - 1

    @property
    def has_prev(self) -> bool:
        """True if a previous page exists"""
        return self.page > 1

    async def next(self, ) -> List[RowProxy]:
        """Returns a :class:`Pagination` object for the next page."""
        self._select_query.limit(self.per_page)
        self._select_query.offset(self.page - 1 + 1 * self.per_page)
        return await self._db_client._find_data(self._select_query)

    @property
    def has_next(self) -> bool:
        """True if a next page exists."""
        return self.page < self.pages

    @property
    def next_num(self) -> int:
        """Number of the next page"""
        if not self.has_next:
            return None
        return self.page + 1


# noinspection PyProtectedMember
class Session(object):
    """
    query session
    """

    def __init__(self, aio_engine, message: Dict, msg_zh: str, max_per_page: int):
        """
            query session
        Args:

        """
        self.aio_engine = aio_engine
        self.message = message
        self.msg_zh = msg_zh
        self.max_per_page = max_per_page

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

    async def _execute(self, query: Union[Select, str], params: Union[List[Dict], Dict], msg_code: int
                       ) -> ResultProxy:
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

    async def _query_execute(self, query: Union[Select, str], params: Dict = None) -> ResultProxy:
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
            cursor = await self._execute(query, insert_data_, 1)
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
            cursor = await self._execute(query, {}, 1)
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
            cursor = await self._execute(query, insert_data_, 1)
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

            if query._bind_values is None:
                select_query = update(model).values(update_data)
            else:
                select_query = update(model).values(query._bind_values)
            for one_clause in query._whereclause:
                select_query = select_query.where(one_clause)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            cursor = await self._execute(select_query, update_data_, 2)
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
                select_query = select_query.where(one_clause)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            async with self.aio_engine.acquire() as conn:
                async with conn.begin() as trans:
                    try:
                        cursor = await conn.execute(select_query)
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
            return cursor.rowcount

    async def _find_one(self, model: DeclarativeMeta, query: BaseQuery) -> Optional[RowProxy]:
        """
        查询单条数据
        Args:
            model: sqlalchemy中的model或者table
            query: BaseQuery查询类
        Returns:
            返回匹配的数据或者None
        """
        try:
            select_query = select([model] if not query._columns else query._columns)
            if query._with_hint:
                select_query = select_query.with_hint(*query._with_hint)
            for one_clause in query._whereclause:
                select_query.append_whereclause(one_clause)
            if query._order_by:
                select_query.append_order_by(*query._order_by)
            if query._distinct:
                select_query = select_query.distinct(*query._distinct)
            if query._columns:
                select_query = select_query.with_only_columns(query._columns)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            cursor = await self._query_execute(select_query)
            return await cursor.first() if cursor.returns_rows else None

    async def _find_data(self, select_query: Select) -> List[RowProxy]:
        """
        查询单条数据
        Args:
            select_query: sqlalchemy中的select query
        Returns:
            返回匹配的数据或者None
        """
        cursor = await self._query_execute(select_query)
        return await cursor.fetchall() if cursor.returns_rows else []

    async def _find_count(self, select_query: Select) -> RowProxy:
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
    def gen_query(select_query: Select, query: BaseQuery, *, limit_clause: int = None,
                  offset_clause: int = None) -> Select:
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
            select_query = select_query.with_hint(*query._with_hint)
        if query._whereclause:
            for one_clause in query._whereclause:
                select_query.append_whereclause(one_clause)
        if query._order_by:
            select_query.append_order_by(*query._order_by)
        if query._group_by:
            select_query.append_group_by(*query._group_by)
            for one_clause in query._having:
                select_query.append_having(one_clause)
        if query._distinct:
            select_query = select_query.distinct(*query._distinct)
        if limit_clause is not None:
            select_query = select_query.limit(limit_clause)
        if offset_clause is not None:
            select_query = select_query.offset(offset_clause)

        return select_query

    async def execute(self, query: Union[Select, str], params: Union[List[Dict], Dict]) -> int:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: 执行的参数值,可以是单个对象的字典也可以是多个对象的列表
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        params = dict(params) if isinstance(params, MutableMapping) else {}
        cursor = await self._execute(query, params, 6)
        return cursor.rowcount

    async def query_execute(self, query: Union[Select, str], params: Dict = None, size=None, cursor_close=True
                            ) -> Union[List[RowProxy], RowProxy, None]:
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

    async def find_one(self, model: DeclarativeMeta, *, query: BaseQuery = None) -> Optional[RowProxy]:
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
                        page: int = 1, per_page: int = 20, primary_order: bool = True) -> Pagination:
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
            select_query = select([model] if not query._columns else query._columns)
            # 如果per_page为0,则证明要获取所有的数据，否则还是通常的逻辑
            if per_page != 0:
                select_query = self.gen_query(
                    select_query, query, limit_clause=per_page, offset_clause=(page - 1) * per_page)
                # 如果分页获取的时候没有进行排序,并且model中有id字段,则增加用id字段的升序排序
                # 前提是默认id是主键,因为不排序会有混乱数据,所以从中间件直接解决,业务层不需要关心了
                # 如果业务层有排序了，则此处不再提供排序功能
                # 如果遇到大数据量的分页查询问题时，建议关闭此处，然后再基于已有的索引分页
                if primary_order is True and getattr(model, "id", None) is not None:
                    select_query.append_order_by(getattr(model, "id").asc())
            else:
                select_query = self.gen_query(select_query, query)
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

    async def find_all(self, model: DeclarativeMeta, *, query: BaseQuery = None) -> List[RowProxy]:
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
            select_query = select([model] if not query._columns else query._columns)
            select_query = self.gen_query(select_query, query)
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
            select_query = self.gen_query(select_query, query)
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

    async def update_data(self, model, *, query: BaseQuery, update_data: Union[Dict, List]) -> int:
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


class AIOMysqlClient(object):
    """
    MySQL异步操作指南
    """
    Model = declarative_base()

    def __init__(self, app=None, *, username: str = "root", passwd: str = None, host: str = "127.0.0.1",
                 port: int = 3306, dbname: str = None, pool_size: int = 10, **kwargs):
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
            pool_recycle: pool recycle time, type int
            aclients_binds: binds config, eg:{"first":{"aclients_mysql_host":"127.0.0.1",
                                                        "aclients_mysql_port":3306,
                                                        "aclients_mysql_username":"root",
                                                        "aclients_mysql_passwd":"",
                                                        "aclients_mysql_dbname":"dbname",
                                                        "aclients_mysql_pool_size":10}}
        """
        self.app = app
        self.engine_pool = {}  # engine pool
        self.session_pool = {}  # session pool
        # default bind connection
        self.username = username
        self.passwd = passwd
        self.host = host
        self.port = port
        self.dbname = dbname
        self.pool_size = pool_size
        # other info
        self.pool_recycle = kwargs.get("pool_recycle", 3600)  # free close time
        self.charset = "utf8mb4"
        self.aclients_binds: Dict = kwargs.get("aclients_binds")  # binds config
        self.message = kwargs.get("message", {})
        self.use_zh = kwargs.get("use_zh", True)
        self.max_per_page = kwargs.get("max_per_page", None)
        self.msg_zh = None

        if app is not None:
            self.init_app(app, username=self.username, passwd=self.passwd, host=self.host, port=self.port,
                          dbname=self.dbname, pool_size=self.pool_size, **kwargs)

    def init_app(self, app, *, username: str = None, passwd: str = None, host: str = None, port: int = None,
                 dbname: str = None, pool_size: int = None, **kwargs):
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
        self.pool_size = pool_size or app.config.get("ACLIENTS_MYSQL_POOL_SIZE", None) or self.pool_size

        self.pool_recycle = kwargs.get("pool_recycle") or app.config.get(
            "ACLIENTS_POOL_RECYCLE", None) or self.pool_recycle

        message = kwargs.get("message") or app.config.get("ACLIENTS_MYSQL_MESSAGE", None) or self.message
        use_zh = kwargs.get("use_zh") or app.config.get("ACLIENTS_MYSQL_MSGZH", None) or self.use_zh

        self.aclients_binds = kwargs.get("aclients_binds") or app.config.get(
            "ACLIENTS_BINDS", None) or self.aclients_binds
        self.verify_binds()

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
            self.engine_pool[None] = await create_engine(
                user=username, db=dbname, host=host, port=port, password=passwd, maxsize=self.pool_size,
                pool_recycle=self.pool_recycle, charset=self.charset)

        @app.listener('after_server_stop')
        async def close_connection(app_, loop):
            """

            Args:

            Returns:

            """
            tasks = []
            for _, aio_engine in self.engine_pool.items():
                aio_engine.close()
                tasks.append(asyncio.ensure_future(aio_engine.wait_closed()))
            await asyncio.wait(tasks)

    def init_engine(self, *, username: str = "root", passwd: str = None, host: str = "127.0.0.1", port: int = 3306,
                    dbname: str = None, pool_size: int = 50, **kwargs):
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
        self.pool_size = pool_size or self.pool_size

        self.pool_recycle = kwargs.get("pool_recycle") or self.pool_recycle

        message = kwargs.get("message") or self.message
        use_zh = kwargs.get("use_zh") or self.use_zh

        self.aclients_binds = kwargs.get("aclients_binds") or self.aclients_binds
        self.verify_binds()

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
            self.engine_pool[None] = await create_engine(
                host=host, port=port, user=username, password=passwd, db=dbname, maxsize=self.pool_size,
                pool_recycle=self.pool_recycle, charset=self.charset)

        async def close_connection():
            """

            Args:

            Returns:

            """
            tasks = []
            for _, aio_engine in self.engine_pool.items():
                aio_engine.close()
                tasks.append(asyncio.ensure_future(aio_engine.wait_closed(), loop=loop))
            await asyncio.wait(tasks)

        loop.run_until_complete(open_connection())
        atexit.register(lambda: loop.run_until_complete(close_connection()))

    def verify_binds(self, ):
        """
        校验aclients_binds
        Args:

        Returns:

        """
        if self.aclients_binds:
            if not isinstance(self.aclients_binds, dict):
                raise TypeError("aclients_binds type error, must be Dict.")
            for bind_name, bind in self.aclients_binds.items():
                if not isinstance(bind, dict):
                    raise TypeError(f"aclients_binds config {bind_name} type error, must be Dict.")
                missing_items = []
                for item in ["aclients_mysql_host", "aclients_mysql_port", "aclients_mysql_username",
                             "aclients_mysql_passwd", "aclients_mysql_dbname"]:
                    if item not in bind:
                        missing_items.append(item)
                if missing_items:
                    raise ConfigError(f"aclients_binds config {bind_name} error, "
                                      f"missing {' '.join(missing_items)} config item.")

    @property
    def query(self, ) -> BaseQuery:
        """

        Args:

        Returns:

        """
        return BaseQuery()

    @property
    def session(self, ) -> Session:
        """
        session default bind
        Args:

        Returns:

        """
        if None not in self.engine_pool:
            raise ValueError("Default bind is not exist.")
        if None not in self.session_pool:
            self.session_pool[None] = Session(self.engine_pool[None], self.message, self.msg_zh, self.max_per_page)
        return self.session_pool[None]

    def gen_session(self, bind: str) -> Session:
        """
        session bind
        Args:
            bind: engine pool one of connection
        Returns:

        """
        if bind not in self.aclients_binds:
            raise ValueError("bind is not exist, please config it in the ACLIENTS_BINDS.")
        if bind not in self.engine_pool:
            bind_conf: Dict = self.aclients_binds[bind]
            self.engine_pool[bind] = asyncio.get_event_loop().run_until_complete(create_engine(
                host=bind_conf.get("aclients_mysql_host"), port=bind_conf.get("aclients_mysql_port"),
                user=bind_conf.get("aclients_mysql_username"), password=bind_conf.get("aclients_mysql_passwd"),
                db=bind_conf.get("aclients_mysql_dbname"),
                maxsize=bind_conf.get("aclients_mysql_pool_size") or self.pool_size,
                pool_recycle=self.pool_recycle, charset=self.charset))
        if bind not in self.session_pool:
            self.session_pool[bind] = Session(self.engine_pool[bind], self.message, self.msg_zh, self.max_per_page)
        return self.session_pool[bind]

    def gen_model(self, model_cls, suffix: str = None, **kwargs):
        """
        用于根据现有的model生成新的model类

        主要用于分表的查询和插入
        Args:
            model_cls: 要生成分表的model类
            suffix: 新的model类名的后缀
            kwargs: 其他的参数
        Returns:

        """
        if kwargs:
            aelog.info(kwargs)
        if not issubclass(model_cls, DeclarativeMeta):
            raise ValueError("model_cls must be db.Model type.")

        table_name = f"{getattr(model_cls, '__tablename__', model_cls.__name__)}_{suffix}"
        class_name = f"{gen_class_name(table_name)}Model"
        if getattr(model_cls, "_cache_class", None) is None:
            setattr(model_cls, "_cache_class", {})

        model_cls_ = getattr(model_cls, "_cache_class").get(class_name, None)
        if model_cls_ is None:
            model_fields = {}
            for attr_name, field in model_cls.__dict__.items():
                if isinstance(field, InstrumentedAttribute) and not attr_name.startswith("_"):
                    model_fields[attr_name] = sa.Column(
                        type_=field.type, primary_key=field.primary_key, index=field.index, nullable=field.nullable,
                        default=field.default, onupdate=field.onupdate, unique=field.unique,
                        autoincrement=field.autoincrement, doc=field.doc)
            model_cls_ = type(class_name, (self.Model,), {
                "__doc__": model_cls.__doc__,
                "__table_args__ ": getattr(
                    model_cls, "__table_args__", None) or {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'},
                "__tablename__": table_name,
                "__module__": model_cls.__module__,
                **model_fields})
            getattr(model_cls, "_cache_class")[class_name] = model_cls_

        return model_cls_
