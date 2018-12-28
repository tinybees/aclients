#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午3:41
"""

from collections.abc import MutableMapping, MutableSequence

import aelog
# noinspection PyProtectedMember
from bson import ObjectId
from bson.errors import BSONError
from motor.motor_asyncio import AsyncIOMotorClient
# noinspection PyPackageRequirements
from pymongo.errors import (ConnectionFailure, DuplicateKeyError, InvalidName, PyMongoError)

from aclients.utils import verify_message
from .err_msg import mongo_msg
from .exceptions import FuncArgsError, HttpError, MongoDuplicateKeyError, MongoError, MongoInvalidNameError

__all__ = ("AIOMongoClient",)


class AIOMongoClient(object):
    """
    mongo 非阻塞工具类
    """

    def __init__(self, app=None, *, username="mongo", passwd=None, host="127.0.0.1", port=27017, dbname=None,
                 pool_size=50, **kwargs):
        """
        mongo 非阻塞工具类
        Args:
            app: app应用
            host:mongo host
            port:mongo port
            dbname: database name
            username: mongo user
            passwd: mongo password
            pool_size: mongo pool size
        """
        self.client = None
        self.db = None
        self.message = None
        self.msg_zh = None

        if app is not None:
            self.init_app(app, username=username, passwd=passwd, host=host, port=port, dbname=dbname,
                          pool_size=pool_size, **kwargs)

    def init_app(self, app, *, username="mongo", passwd=None, host="127.0.0.1", port=27017, dbname=None,
                 pool_size=50, **kwargs):
        """
        mongo 实例初始化
        Args:
            app: app应用
            host:mongo host
            port:mongo port
            dbname: database name
            username: mongo user
            passwd: mongo password
            pool_size: mongo pool size
        Returns:

        """
        username = app.config.get("ACLIENTS_MONGO_USERNAME", None) or username
        passwd = app.config.get("ACLIENTS_MONGO_PASSWD", None) or passwd
        host = app.config.get("ACLIENTS_MONGO_HOST", None) or host
        port = app.config.get("ACLIENTS_MONGO_PORT", None) or port
        dbname = app.config.get("ACLIENTS_MONGO_DBNAME", None) or dbname
        pool_size = app.config.get("ACLIENTS_MONGO_POOL_SIZE", None) or pool_size
        message = app.config.get("ACLIENTS_MONGO_MESSAGE", None) or kwargs.get("message")
        use_zh = app.config.get("ACLIENTS_MONGO_MSGZH", None) or kwargs.get("use_zh", True)

        passwd = passwd if passwd is None else str(passwd)
        self.message = verify_message(mongo_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"

        @app.listener('before_server_start')
        def open_connection(app_, loop):
            """

            Args:

            Returns:

            """
            try:
                self.client = AsyncIOMotorClient(host, port, maxPoolSize=pool_size, username=username, password=passwd)
                self.db = self.client.get_database(name=dbname)
            except ConnectionFailure as e:
                aelog.exception("Mongo connection failed host={} port={} error:{}".format(host, port, e))
                raise MongoError("Mongo connection failed host={} port={} error:{}".format(host, port, e))
            except InvalidName as e:
                aelog.exception("Invalid mongo db name {} {}".format(dbname, e))
                raise MongoInvalidNameError("Invalid mongo db name {} {}".format(dbname, e))
            except PyMongoError as err:
                aelog.exception("Mongo DB init failed! error: {}".format(err))
                raise MongoError("Mongo DB init failed!") from err

        @app.listener('after_server_stop')
        def close_connection(app_, loop):
            """

            Args:

            Returns:

            """
            self.client.close()

    async def _insert_document(self, name, document, insert_one=True):
        """
        插入一个单独的文档
        Args:
            name:collection name
            document: document obj
            insert_one: insert_one insert_many的过滤条件，默认True
        Returns:
            返回插入的Objectid
        """
        try:
            if insert_one:
                result = await self.db.get_collection(name).insert_one(document)
            else:
                result = await self.db.get_collection(name).insert_many(document)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except DuplicateKeyError as e:
            raise MongoDuplicateKeyError("Duplicate key error, {}".format(e))
        except PyMongoError as err:
            aelog.exception("Insert one document failed, {}".format(err))
            raise HttpError(400, message=self.message[100][self.msg_zh])
        else:
            return str(result.inserted_id) if insert_one else (str(val) for val in result.inserted_ids)

    async def _insert_documents(self, name, documents):
        """
        批量插入文档
        Args:
            name:collection name
            documents: documents obj
        Returns:
            返回插入的Objectid列表
        """
        return await self._insert_document(name, documents, insert_one=False)

    async def _find_document(self, name, query_key, filter_key=None):
        """
        查询一个单独的document文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
        Returns:
            返回匹配的document或者None
        """
        try:
            find_data = await self.db.get_collection(name).find_one(query_key, projection=filter_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Find one document failed, {}".format(err))
            raise HttpError(400, message=self.message[103][self.msg_zh])
        else:
            if find_data and find_data.get("_id", None) is not None:
                find_data["id"] = str(find_data.pop("_id"))
            return find_data

    async def _find_documents(self, name, query_key, filter_key=None, limit=None, skip=None, sort=None):
        """
        批量查询documents文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
            limit: 限制返回的document条数
            skip: 从查询结果中调过指定数量的document
            sort: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        Returns:
            返回匹配的document列表
        """
        try:
            find_data = []
            cursor = self.db.get_collection(name).find(query_key, projection=filter_key, limit=limit, skip=skip,
                                                       sort=sort)
            # find_data = await cursor.to_list(None)
            async for doc in cursor:
                if doc.get("_id", None) is not None:
                    doc["id"] = str(doc.pop("_id"))
                find_data.append(doc)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Find many documents failed, {}".format(err))
            raise HttpError(400, message=self.message[104][self.msg_zh])
        else:
            return find_data

    async def _find_count(self, name, query_key):
        """
        查询documents的数量
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回匹配的document数量
        """
        try:
            return await self.db.get_collection(name).count(query_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Find many documents failed, {}".format(err))
            raise HttpError(400, message=self.message[104][self.msg_zh])

    async def _update_document(self, name, query_key: dict, update_data: dict, upsert=False, update_one=True):
        """
        更新匹配到的一个的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
            update_one: update_one or update_many的匹配条件
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 1, "modified_count": 1, "upserted_id":"f"}
        """
        # $set用的比较多，这里默认做个封装
        if update_data and ("$" not in list(update_data.keys())[0]):
            update_data = {"$set": update_data}
        try:
            if update_one:
                result = await self.db.get_collection(name).update_one(query_key, update_data, upsert=upsert)
            else:
                result = await self.db.get_collection(name).update_many(query_key, update_data, upsert=upsert)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except DuplicateKeyError as e:
            raise MongoDuplicateKeyError("Duplicate key error, {}".format(e))
        except PyMongoError as err:
            aelog.exception("Update documents failed, {}".format(err))
            raise HttpError(400, message=self.message[101][self.msg_zh])
        else:
            return {"matched_count": result.matched_count, "modified_count": result.modified_count,
                    "upserted_id": str(result.upserted_id) if result.upserted_id else None}

    async def _update_documents(self, name, query_key: dict, update_data: dict, upsert=False):
        """
        更新匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 2, "modified_count": 2, "upserted_id":"f"}
        """
        return await self._update_document(name, query_key, update_data, upsert, update_one=False)

    async def _delete_document(self, name, query_key, delete_one=True):
        """
        删除匹配到的一个的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            delete_one: delete_one delete_many的匹配条件
        Returns:
            返回删除的数量
        """
        try:
            if delete_one:
                result = await self.db.get_collection(name).delete_one(query_key)
            else:
                result = await self.db.get_collection(name).delete_many(query_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Delete documents failed, {}".format(err))
            raise HttpError(400, message=self.message[102][self.msg_zh])
        else:
            return result.deleted_count

    async def _delete_documents(self, name, query_key):
        """
        删除匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return await self._delete_document(name, query_key, delete_one=False)

    async def _aggregate(self, name, pipline):
        """
        根据pipline进行聚合查询
        Args:
            name: collection name
            pipline: 聚合查询的pipeline,包含一个后者多个聚合命令
        Returns:
            返回聚合后的documents
        """
        result = []
        try:
            async for doc in self.db.get_collection(name).aggregate(pipline):
                if doc.get("_id", None) is not None:
                    doc["id"] = str(doc.pop("_id"))
                result.append(doc)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Aggregate documents failed, {}".format(err))
            raise HttpError(400, message=self.message[105][self.msg_zh])
        else:
            return result

    async def insert_documents(self, name: str, documents: dict):
        """
        批量插入文档
        Args:
            name:collection name
            documents: documents obj
        Returns:
            返回插入的转换后的_id列表
        """
        if not isinstance(documents, MutableSequence):
            aelog.error("insert many document failed, documents is not a iterable type.")
            raise MongoError("insert many document failed, documents is not a iterable type.")
        documents = list(documents)
        for document in documents:
            if not isinstance(document, MutableMapping):
                aelog.error("insert one document failed, document is not a mapping type.")
                raise MongoError("insert one document failed, document is not a mapping type.")
            if document and "id" in document:
                try:
                    document["_id"] = ObjectId(document.pop("id"))
                except BSONError as e:
                    raise FuncArgsError(str(e))
        return await self._insert_documents(name, documents)

    async def insert_document(self, name: str, document: dict):
        """
        插入一个单独的文档
        Args:
            name:collection name
            document: document obj
        Returns:
            返回插入的转换后的_id
        """
        document = dict(document)
        if document and "id" in document:
            try:
                document["_id"] = ObjectId(document.pop("id"))
            except BSONError as e:
                raise FuncArgsError(str(e))
        if not isinstance(document, MutableMapping):
            aelog.error("insert one document failed, document is not a mapping type.")
            raise MongoError("insert one document failed, document is not a mapping type.")
        document = dict(document)
        return await self._insert_document(name, document)

    async def find_document(self, name: str, query_key: dict = None, filter_key: dict = None):
        """
        查询一个单独的document文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
        Returns:
            返回匹配的document或者None
        """
        if query_key and "id" in query_key:
            try:
                query_key["_id"] = ObjectId(query_key.pop("id"))
            except BSONError as e:
                raise FuncArgsError(str(e))
        return await self._find_document(name, query_key, filter_key=filter_key)

    async def find_documents(self, name: str, query_key: dict = None, filter_key: dict = None, limit=0, page=1,
                             sort=None):
        """
        批量查询documents文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
            limit: 每页数据的数量
            page: 查询第几页的数据
            sort: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        Returns:
            返回匹配的document列表
        """
        if query_key and "id" in query_key:
            try:
                query_key["_id"] = ObjectId(query_key.pop("id"))
            except BSONError as e:
                raise FuncArgsError(str(e))
        skip = (int(page) - 1) * int(limit)
        return await self._find_documents(name, query_key, filter_key=filter_key, limit=int(limit), skip=skip,
                                          sort=sort)

    async def find_count(self, name: str, query_key: dict = None):
        """
        查询documents的数量
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回匹配的document数量
        """
        if query_key and "id" in query_key:
            try:
                query_key["_id"] = ObjectId(query_key.pop("id"))
            except BSONError as e:
                raise FuncArgsError(str(e))
        return await self._find_count(name, query_key)

    async def update_documents(self, name: str, query_key: dict, update_data: dict, upsert: bool = False):
        """
        更新匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 2, "modified_count": 2, "upserted_id":"f"}
        """
        update_data = dict(update_data)
        if query_key and "id" in query_key:
            try:
                query_key["_id"] = ObjectId(query_key.pop("id"))
            except BSONError as e:
                raise FuncArgsError(str(e))
        return await self._update_documents(name, query_key, update_data, upsert=upsert)

    async def update_document(self, name: str, query_key: dict, update_data: dict, upsert: bool = False):
        """
        更新匹配到的一个的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 1, "modified_count": 1, "upserted_id":"f"}
        """
        update_data = dict(update_data)
        if query_key and "id" in query_key:
            try:
                query_key["_id"] = ObjectId(query_key.pop("id"))
            except BSONError as e:
                raise FuncArgsError(str(e))
        return await self._update_document(name, query_key, update_data, upsert=upsert)

    async def delete_documents(self, name: str, query_key: dict):
        """
        删除匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        if query_key and "id" in query_key:
            try:
                query_key["_id"] = ObjectId(query_key.pop("id"))
            except BSONError as e:
                raise FuncArgsError(str(e))
        return await self._delete_documents(name, query_key)

    async def delete_document(self, name: str, query_key: dict):
        """
        删除匹配到的一个的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        if query_key and "id" in query_key:
            try:
                query_key["_id"] = ObjectId(query_key.pop("id"))
            except BSONError as e:
                raise FuncArgsError(str(e))
        return await self._delete_document(name, query_key)

    async def aggregate(self, name: str, pipline: list, page=None, limit=None):
        """
        根据pipline进行聚合查询
        Args:
            name: collection name
            pipline: 聚合查询的pipeline,包含一个后者多个聚合命令
            limit: 每页数据的数量
            page: 查询第几页的数据
        Returns:
            返回聚合后的documents
        """
        if not isinstance(pipline, MutableSequence):
            aelog.error("Aggregate query failed, pipline arg is not a iterable type.")
            raise MongoError("Aggregate query failed, pipline arg is not a iterable type.")
        if page is not None and limit is not None:
            pipline.extend([{'$skip': (int(page) - 1) * int(limit)}, {'$limit': int(limit)}])
        return await self._aggregate(name, pipline)
