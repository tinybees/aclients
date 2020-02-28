#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/2/28 下午3:30
"""
import unittest
from datetime import datetime

import sqlalchemy as sa

from aclients import AIOMysqlClient
from aclients.utils import objectid

mysql_db = AIOMysqlClient()


class MessageDisplayModel(mysql_db.Model):
    """
    消息展示
    """
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}

    __tablename__ = "message_display"

    id = sa.Column(sa.String(24), primary_key=True, default=objectid, nullable=False, doc='实例ID')
    msg_code = sa.Column(sa.Integer, index=True, unique=True, nullable=False, doc='消息编码')
    msg_zh = sa.Column(sa.String(200), unique=True, nullable=False, doc='中文消息')
    msg_en = sa.Column(sa.String(200), doc='英文消息')
    description = sa.Column(sa.String(255), doc='描述')
    created_time = sa.Column(sa.DateTime, default=datetime.now, nullable=False, doc='创建时间')
    updated_time = sa.Column(sa.DateTime, default=datetime.now, onupdate=datetime.now, nullable=False, doc='更新时间')


class TestSQL(unittest.TestCase):
    """
    测试Query类中的生成SQL功能
    """

    def test_select_sql(self):
        """
            Args:
        """
        sql = mysql_db.query.model(MessageDisplayModel).where(
            MessageDisplayModel.id == "5e53bb135b64856045ccb8dc").select_query().sql()
        self.assertEqual(sql, {
            'sql': 'SELECT message_display.id, message_display.msg_code, message_display.msg_zh, message_display.msg_en, message_display.description, message_display.created_time, message_display.updated_time \nFROM message_display \nWHERE message_display.id = %(id_1)s',
            'params': {'id_1': '5e53bb135b64856045ccb8dc'}})

    def test_select_count_sql(self, ):
        """
            Args:
        """
        sql = mysql_db.query.model(MessageDisplayModel).where(
            MessageDisplayModel.id == "5e53bb135b64856045ccb8dc").select_query(True).sql()
        self.assertEqual(sql, {
            'sql': 'SELECT count(*) AS count \nFROM message_display \nWHERE message_display.id = %(id_1)s',
            'params': {'id_1': '5e53bb135b64856045ccb8dc'}})

    def test_insert_sql(self, ):
        """
            Args:
        """
        sql = mysql_db.query.model(MessageDisplayModel).insert_query({
            "msg_code": 3100, "id": "5e53bb135b64856045ccb8dc", "msg_zh": "fdfdf"}).sql()
        self.assertEqual(sql["sql"],
                         'INSERT INTO message_display (id, msg_code, msg_zh, created_time, updated_time) VALUES (%(id)s, %(msg_code)s, %(msg_zh)s, %(created_time)s, %(updated_time)s)'
                         )
        self.assertEqual(sql["params"]["id"], "5e53bb135b64856045ccb8dc")
        self.assertEqual(sql["params"]["msg_code"], 3100)
        self.assertEqual(sql["params"]["msg_zh"], "fdfdf")

    def test_update_sql(self, ):
        """
            Args:
        """
        sql = mysql_db.query.model(MessageDisplayModel).where(
            MessageDisplayModel.id == "5e53bb135b64856045ccb8dc").update_query({"msg_code": 3100}).sql()
        self.assertEqual(sql["sql"],
                         'UPDATE message_display SET msg_code=%(msg_code)s, updated_time=%(updated_time)s WHERE message_display.id = %(id_1)s')
        self.assertEqual(sql["params"]["id_1"], "5e53bb135b64856045ccb8dc")
        self.assertEqual(sql["params"]["msg_code"], 3100)

    def test_delete_sql(self, ):
        """
            Args:
        """
        sql = mysql_db.query.model(MessageDisplayModel).where(
            MessageDisplayModel.id == "5e53bb135b64856045ccb8dc").delete_query().sql()
        self.assertEqual(sql, {'sql': 'DELETE FROM message_display WHERE message_display.id = %(id_1)s',
                               'params': {'id_1': '5e53bb135b64856045ccb8dc'}})

    def test_paginate_sql(self, ):
        """
            Args:
        """
        sql = mysql_db.query.model(MessageDisplayModel).where(
            MessageDisplayModel.id == "5e53bb135b64856045ccb8dc").paginate_query().sql()
        self.assertEqual(len(sql), 2)
        self.assertEqual(len(sql[0]), 2)
        self.assertEqual(len(sql[1]), 2)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(TestSQL)
