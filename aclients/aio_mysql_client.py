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
from aiomysql.sa import create_engine, exc
# noinspection PyProtectedMember
from aiomysql.sa.connection import _distill_params, noop
# noinspection PyProtectedMember
from aiomysql.sa.engine import _dialect
from aiomysql.sa.exc import Error
from aiomysql.sa.result import ResultProxy, RowProxy
from pymysql.err import IntegrityError, MySQLError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import (Select, all_, and_, any_, asc, bindparam, case, cast, column, delete, desc,
                            distinct, except_, except_all, exists, extract, false, func, funcfilter, insert, intersect,
                            intersect_all, join, label, not_, null, nullsfirst, nullslast, or_, outerjoin, over, select,
                            table, text, true, tuple_, type_coerce, union, union_all, update, within_group)
from sqlalchemy.sql.dml import Delete, Insert, Update, UpdateBase
from sqlalchemy.sql.elements import TextClause

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
        self._model: DeclarativeMeta = None
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
        # limit, offset
        self._limit_clause: int = None
        self._offset_clause: int = None

    def where(self, *whereclause) -> 'BaseQuery':
        """return basequery construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        """

        self._whereclause.extend(whereclause)
        return self

    def model(self, modelclause: DeclarativeMeta) -> 'BaseQuery':
        """
        return basequery construct with the given expression added to
        its model clause.

        Arg:
            modelclause: sqlalchemy中的model或者table
        """
        if not isinstance(modelclause, DeclarativeMeta):
            raise FuncArgsError("model type error!")

        self._model = modelclause
        return self

    table = model

    def order_by(self, *clauses) -> 'BaseQuery':
        """return basequery with the given list of ORDER BY
        criterion applied.

        The criterion will be appended to any pre-existing ORDER BY
        criterion.

        """

        self._order_by.extend(clauses)
        return self

    def group_by(self, *clauses) -> 'BaseQuery':
        """return basequery with the given list of GROUP BY
        criterion applied.

        The criterion will be appended to any pre-existing GROUP BY
        criterion.

        """

        self._group_by.extend(clauses)
        return self

    def having(self, *having) -> 'BaseQuery':
        """return basequery construct with the given expression added to
        its HAVING clause, joined to the existing clause via AND, if any.

        """
        self._having.extend(having)

    def distinct(self, *expr) -> 'BaseQuery':
        r"""Return basequery construct which will apply DISTINCT to its
        columns clause.

        :param expr: optional column expressions.  When present,
         the PostgreSQL dialect will render a ``DISTINCT ON (<expressions>>)``
         construct.

        """
        self._distinct.extend(expr)
        return self

    def columns(self, *columns) -> 'BaseQuery':
        r"""Return basequery :func:`.select` construct with its columns
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

    def union(self, other, **kwargs) -> 'BaseQuery':
        """return a SQL UNION of this select() construct against the given
        selectable."""

        self._union = [other, kwargs]
        return self

    def union_all(self, other, **kwargs) -> 'BaseQuery':
        """return a SQL UNION ALL of this select() construct against the given
        selectable.

        """
        self._union_all = [other, kwargs]
        return self

    def with_hint(self, selectable, text_, dialect_name='*') -> 'BaseQuery':
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

    def values(self, **kwargs) -> 'BaseQuery':
        r"""specify a fixed VALUES clause for an SET clause for an UPDATE."""
        self._bind_values = kwargs
        return self


# noinspection PyProtectedMember
class Query(BaseQuery):
    """
    查询
    """

    def __init__(self, max_per_page: int = None):
        """

        Args:

        Returns:

        """
        # data
        self._insert_data: Union[List[Dict], Dict] = None
        self._update_data: Union[List[Dict], Dict] = None
        # query
        self._query_obj: Union[Select, Insert, Update, Delete] = None
        self._query_count_obj: Select = None  # 查询数量select
        # per page max count
        self._max_per_page: int = max_per_page
        #: the current page number (1 indexed)
        self._page = None
        #: the number of items to be displayed on a page.
        self._per_page = None

        super().__init__()

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

    @staticmethod
    def _base_params(query, dp, compiled, is_update) -> Optional[Dict]:
        """
        handle params
        """
        if dp and isinstance(dp, (list, tuple)):
            if is_update:
                dp = {c.key: pval for c, pval in zip(query.table.c, dp)}
            else:
                raise exc.ArgumentError("Don't mix sqlalchemy SELECT clause with positional parameters")
        compiled_params = compiled.construct_params(dp)
        processors = compiled._bind_processors
        params = [{
            key: processors.get(key, noop)(compiled_params[key])
            for key in compiled_params
        }]
        post_processed_params = _dialect.execute_sequence_format(params)
        return post_processed_params[0]

    def _compiled_quey(self, query: Union[Select, Insert, Update, Delete], *multiparams: Union[Dict, List[Dict]]
                       ) -> Dict[str, Union[str, Dict, List[Dict], None]]:
        """
        compile query to sql
        Args:
            query:
            multiparams:
        Returns:
            {"sql": sql, "params": params}
        """
        bind_params = _distill_params(multiparams, {})

        if isinstance(query, str):
            query_, params_ = (query, bind_params) if len(bind_params) > 1 else (query, bind_params or None)
        else:
            compiled = query.compile(dialect=_dialect)
            query_, params_ = str(compiled), None

            if len(bind_params) > 1:
                params_ = []
                for bind_param in bind_params:
                    params_.append(self._base_params(query, bind_param, compiled, isinstance(query, UpdateBase)))
            elif bind_params:
                bind_params = bind_params[0]
            params_ = self._base_params(query, bind_params, compiled, isinstance(query, UpdateBase))

        return {"sql": query_, "params": params_}

    def _verify_model(self, ):
        """

        Args:

        Returns:

        """
        if self._model is None:
            raise FuncArgsError("Query 对象中缺少Model")

    def insert_from_query(self, column_names: List, query: 'Query') -> 'Query':
        """
        查询并且插入数据, ``INSERT...FROM SELECT`` statement.

        e.g.::
            sel = select([table1.c.a, table1.c.b]).where(table1.c.c > 5)
            ins = table2.insert().from_select(['a', 'b'], sel)
        Args:
            column_names: 字符串列名列表或者Column类列名列表
            query: Query类
        Returns:
            (插入的条数,插入的ID)
        """
        self._verify_model()
        try:
            self._query_obj = insert(self._model).from_select(column_names, query._query_obj)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            return self

    def insert_query(self, insert_data: Union[List[Dict], Dict]) -> 'Query':
        """
        insert query
        Args:
            insert_data: 值类型Dict or List[Dict]
        Returns:
            Select object
        """
        self._verify_model()
        try:
            insert_data_ = self._get_model_default_value(self._model)
            if isinstance(insert_data, dict):
                insert_data_.update(insert_data)
            else:
                insert_data_ = [{**insert_data_, **one_data} for one_data in insert_data]
            query = insert(self._model).values(insert_data_)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            self._query_obj, self._insert_data = query, insert_data_
            return self

    def update_query(self, update_data: Union[List[Dict], Dict]) -> 'Query':
        """
        update query

        eg: where(User.c.id == bindparam("id")).values({"name": bindparam("name")})
         await conn.execute(sql, [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}]
        Args:
            update_data: 值类型
        Returns:
            返回更新的条数
        """
        self._verify_model()
        try:
            update_data_ = self._get_model_onupdate_value(self._model)
            if isinstance(update_data, MutableMapping):
                update_data_ = {**update_data_, **update_data}
            else:
                update_data_ = [{**update_data_, **one_data} for one_data in update_data]

            if self._bind_values is None:
                query = update(self._model).values(update_data_)
            else:
                query = update(self._model).values(self._bind_values)
            for one_clause in self._whereclause:
                query = query.where(one_clause)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            self._query_obj, self._update_data = query, update_data_
            return self

    def delete_query(self, ) -> 'Query':
        """
        delete query
        Args:
        Returns:
            返回删除的条数
        """
        self._verify_model()
        try:
            query = delete(self._model)
            for one_clause in self._whereclause:
                query = query.where(one_clause)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            self._query_obj = query
            return self

    def select_query(self, is_count: bool = False) -> 'Query':
        """
        select query
        Args:
            is_count: 是否为数量查询
        Returns:
            返回匹配的数据或者None
        """
        try:
            if is_count is False:
                query = select([self._model] if not self._columns else self._columns)
                # 以下的查询只有普通查询才有，和查询数量么有关系
                if self._order_by:
                    query.append_order_by(*self._order_by)
                if self._columns:
                    query = query.with_only_columns(self._columns)
                if self._limit_clause is not None:
                    query = query.limit(self._limit_clause)
                if self._offset_clause is not None:
                    query = query.offset(self._offset_clause)
            else:
                query = select([func.count().label("count")]).select_from(self._model)
            # 以下的查询条件都会有
            if self._with_hint:
                query = query.with_hint(*self._with_hint)
            if self._whereclause:
                for one_clause in self._whereclause:
                    query.append_whereclause(one_clause)
            if self._group_by:
                query.append_group_by(*self._group_by)
                for one_clause in self._having:
                    query.append_having(one_clause)
            if self._distinct:
                query = query.distinct(*self._distinct)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            if is_count is False:
                self._query_obj = query
            else:
                self._query_count_obj = query
            return self

    def paginate_query(self, *, page: int = 1, per_page: int = 20,
                       primary_order: bool = True) -> 'Query':
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

        目前是改造如果limit传递为0，则返回所有的数据，这样业务代码中就不用更改了
        """
        if self._max_per_page is not None:
            per_page = min(per_page, self._max_per_page)

        if page < 1:
            page = 1

        if per_page < 0:
            per_page = 20

        self._page, self._per_page = page, per_page

        try:
            # 如果per_page为0,则证明要获取所有的数据，否则还是通常的逻辑
            if per_page != 0:
                self._limit_clause = per_page
                self._offset_clause = (page - 1) * per_page
                # 如果分页获取的时候没有进行排序,并且model中有id字段,则增加用id字段的升序排序
                # 前提是默认id是主键,因为不排序会有混乱数据,所以从中间件直接解决,业务层不需要关心了
                # 如果业务层有排序了，则此处不再提供排序功能
                # 如果遇到大数据量的分页查询问题时，建议关闭此处，然后再基于已有的索引分页
                if primary_order is True and getattr(self._model, "id", None) is not None:
                    self.order_by(getattr(self._model, "id").asc())

            self.select_query()  # 生成select SQL
            self.select_query(is_count=True)  # 生成select count SQL
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            return self

    def insert_sql(self, insert_data: Union[List[Dict], Dict]) -> Dict[str, Union[str, Dict, List[Dict], None]]:
        """
        insert sql
        Args:
            insert_data: 值类型Dict or List[Dict]
        Returns:
            {"sql": "insert sql", "params": "insert data"}
        """
        self.insert_query(insert_data)
        return self._compiled_quey(self._query_obj, self._insert_data)

    def update_sql(self, update_data: Union[List[Dict], Dict]) -> Dict[str, Union[str, Dict, List[Dict], None]]:
        """
        update sql

        Args:
            update_data: 值类型Dict or List[Dict]
        Returns:
            {"sql": "update sql", "params": "update data"}
        """
        self.update_query(update_data)
        return self._compiled_quey(self._query_obj, self._update_data)

    def delete_sql(self, ) -> Dict[str, Union[str, Dict, List[Dict], None]]:
        """
        delete sql
        Args:
        Returns:
            {"sql": "delete sql", "params": "delete params"}
        """
        self.delete_query()
        return self._compiled_quey(self._query_obj)

    def select_sql(self, is_count: bool = False) -> Dict[str, Union[str, Dict, List[Dict], None]]:
        """
        select sql
        Args:
            is_count: 是否为查询数量
        Returns:
            {"sql": "select sql", "params": "select data"}
        """
        if is_count is False:
            self.select_query()
            result = self._compiled_quey(self._query_obj)
        else:
            self.select_query(is_count=True)
            result = self._compiled_quey(self._query_count_obj)
        return result

    def paginate_sql(self, *, page: int = 1, per_page: int = 20,
                     primary_order: bool = True) -> List[Dict[str, Union[str, Dict, List[Dict], None]]]:
        """
        If ``page`` or ``per_page`` are ``None``, they will be retrieved from
        the request query. If ``max_per_page`` is specified, ``per_page`` will
        be limited to that value. If there is no request or they aren't in the
        query, they default to 1 and 20 respectively.
        目前是改造如果limit传递为0，则返回所有的数据，这样业务代码中就不用更改了

        Args:
            page: page is less than 1, or ``per_page`` is negative.
            per_page: page or per_page are not ints.
            primary_order: 默认启用主键ID排序的功能，在大数据查询时可以关闭此功能，在90%数据量不大的情况下可以加快分页的速度

            When ``error_out`` is ``False``, ``page`` and ``per_page`` default to
            1 and 20 respectively.

        Returns:
            [{"sql": "select sql", "params": "select params"},
            {"sql": "select count sql", "params": "select count params"}]
        """
        self.paginate_query(page=page, per_page=per_page, primary_order=primary_order)
        select_sql = self._compiled_quey(self._query_obj)
        select_count_sql = self._compiled_quey(self._query_count_obj)
        return [select_sql, select_count_sql]


# noinspection PyProtectedMember
class Pagination(object):
    """Internal helper class returned by :meth:`BaseQuery.paginate`.  You
    can also construct it from any other SQLAlchemy query object if you are
    working with other libraries.  Additionally it is possible to pass `None`
    as query object in which case the :meth:`prev` and :meth:`next` will
    no longer work.
    """

    def __init__(self, db_client: 'Session', query: Query, total: int, items: List[Dict]):
        #: the unlimited query object that was used to create this
        #: aiomysqlclient object.
        self._db_client: Session = db_client
        #: select query
        self._query: Query = query
        #: the current page number (1 indexed)
        self.page: int = query._page
        #: the number of items to be displayed on a page.
        self.per_page: int = query._per_page
        #: the total number of items matching the query
        self.total: int = total
        #: the items for the current page
        self.items: List[Dict] = items

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
        self._query._offset_clause = (self.page - 1 - 1) * self.per_page
        self._query.select_query()  # 重新生成分页SQL
        return await self._db_client._find_data(self._query)

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
        self._query._offset_clause = self.page - 1 + 1 * self.per_page
        self._query.select_query()  # 重新生成分页SQL
        return await self._db_client._find_data(self._query)

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

    async def _execute(self, query: Union[Insert, Update, str], params: Union[List[Dict], Dict], msg_code: int
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

    async def _delete_execute(self, query: Union[Delete, str]) -> int:
        """
        删除数据
        Args:
            query: Query 查询类
        Returns:
            返回删除的条数
        """
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
                    raise HttpError(400, message=self.message[3][self.msg_zh])
                else:
                    await trans.commit()
        return cursor.rowcount

    async def _find_data(self, query: Query) -> List[RowProxy]:
        """
        查询单条数据
        Args:
            query: Query 查询类
        Returns:
            返回匹配的数据或者None
        """
        cursor = await self._query_execute(query._query_obj)
        return await cursor.fetchall() if cursor.returns_rows else []

    async def execute(self, query: Union[TextClause, str], params: Union[List[Dict], Dict]) -> int:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串
            params: 执行的参数值,可以是单个对象的字典也可以是多个对象的列表
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        params = dict(params) if isinstance(params, MutableMapping) else {}
        cursor = await self._execute(query, params, 6)
        return cursor.rowcount

    async def query_execute(self, query: Union[TextClause, str], params: Dict = None, size=None, cursor_close=True
                            ) -> Union[List[RowProxy], RowProxy, None]:
        """
        查询数据，用于复杂的查询
        Args:
            query: SQL的查询字符串
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

    async def insert_one(self, query: Query) -> Tuple[int, str]:
        """
        插入数据
        Args:
           query: Query 查询类
        Returns:
            (插入的条数,插入的ID)
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._execute(query._query_obj, query._insert_data, 1)
        return cursor.rowcount, query._insert_data.get("id") or cursor.lastrowid

    async def insert_many(self, query: Query) -> int:
        """
        插入多条数据

        eg: User.insert().values([{"name": "test1"}, {"name": "test2"}]
        Args:
           query: Query 查询类
        Returns:
            插入的条数
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._execute(query._query_obj, query._insert_data, 1)
        return cursor.rowcount

    async def insert_from_select(self, query: Query) -> Tuple[int, str]:
        """
        查询并且插入数据, ``INSERT...FROM SELECT`` statement.

        e.g.::
            sel = select([table1.c.a, table1.c.b]).where(table1.c.c > 5)
            ins = table2.insert().from_select(['a', 'b'], sel)
        Args:
            query: Query 查询类
        Returns:
            (插入的条数,插入的ID)
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._execute(query._query_obj, {}, 1)
        return cursor.rowcount, cursor.lastrowid

    async def find_one(self, query: Query) -> Optional[RowProxy]:
        """
        查询单条数据
        Args:
            query: Query 查询类
        Returns:
            返回匹配的数据或者None
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._query_execute(query._query_obj)
        return await cursor.first() if cursor.returns_rows else None

    async def find_many(self, query: Query = None) -> Pagination:
        """
        查询多条数据,分页数据
        Args:
            query: Query 查询类
        Returns:
            Returns a :class:`Pagination` object.
        """

        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        items = await self._find_data(query)

        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if query._page == 1 and len(items) < query._per_page:
            total = len(items)
        else:
            total_result = await self.find_count(query)
            total = total_result.count

        return Pagination(self, query, total, items)

    async def find_all(self, query: Query) -> List[RowProxy]:
        """
        插入数据
        Args:
            query: Query 查询类
        Returns:

        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        return await self._find_data(query)

    async def find_count(self, query: Query) -> RowProxy:
        """
        查询数量
        Args:
            query: Query 查询类
        Returns:
            返回条数
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._query_execute(query._query_count_obj)
        return await cursor.first()

    async def update_data(self, query: Query) -> int:
        """
        更新数据

        eg: where(User.c.id == bindparam("id")).values({"name": bindparam("name")})
         await conn.execute(sql, [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}]
        Args:
            query: Query 查询类
        Returns:
            返回更新的条数
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._execute(query._query_obj, query._update_data, 2)
        return cursor.rowcount

    async def delete_data(self, query: Query) -> int:
        """
        删除数据
        Args:
            query: Query 查询类
        Returns:
            返回删除的条数
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        return await self._delete_execute(query._query_obj)


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
    def query(self, ) -> Query:
        """

        Args:

        Returns:

        """
        return Query(self.max_per_page)

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
