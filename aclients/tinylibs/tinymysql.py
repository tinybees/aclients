#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 19-4-2 上午9:04
"""
import aelog
import pymysql

__all__ = ("TinyMysql",)


class TinyMysql(object):
    """
        pymysql 操作数据库的各种方法
    Args:

    Returns:

    """

    def __init__(self, db_host="127.0.0.1", db_port=3306, db_name=None, db_user="root", db_pwd="123456"):
        """
            pymysql 操作数据库的各种方法
        Args:

        Returns:

        """
        self.conn_pool = {}  # 各个不同连接的连接池
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pwd = db_pwd
        self.db_name = db_name

    @property
    def conn(self, ) -> pymysql.Connection:
        """
        获取MySQL的连接对象
        """
        name = "{0}{1}{2}".format(self.db_host, self.db_port, self.db_name)

        def get_connection():
            return pymysql.connect(host=self.db_host, port=self.db_port, db=self.db_name, user=self.db_user,
                                   passwd=self.db_pwd, charset="utf8", cursorclass=pymysql.cursors.DictCursor)

        if not self.conn_pool.get(name):
            self.conn_pool[name] = get_connection()
            return self.conn_pool[name]
        else:
            try:
                self.conn_pool[name].ping()
            except pymysql.Error:
                self.conn_pool[name] = get_connection()
            return self.conn_pool[name]

    def execute_many(self, sql, args_data):
        """
            批量插入数据
        Args:
            sql: 插入的SQL语句
            args_data: 批量插入的数据，为一个包含元祖的列表
        Returns:
        INSERT INTO traffic_100 (IMEI,lbs_dict_id,app_key) VALUES(%s,%s,%s)
        [('868403022323171', None, 'EB23B21E6E1D930E850E7267E3F00095'),
        ('865072026982119', None, 'EB23B21E6E1D930E850E7267E3F00095')]

        """

        count = None
        try:
            with self.conn.cursor() as cursor:
                count = cursor.executemany(sql, args_data)
        except pymysql.Error as e:
            self.conn.rollback()
            aelog.exception(e)
        except Exception as e:
            self.conn.rollback()
            aelog.exception(e)
        else:
            self.conn.commit()
        return count

    def execute(self, sql, args_data):
        """
            执行单条记录，更新、插入或者删除
        Args:
            sql: 插入的SQL语句
            args_data: 批量插入的数据，为一个包含元祖的列表
        Returns:
        INSERT INTO traffic_100 (IMEI,lbs_dict_id,app_key) VALUES(%s,%s,%s)
        ('868403022323171', None, 'EB23B21E6E1D930E850E7267E3F00095')

        """

        count = None
        try:
            with self.conn.cursor() as cursor:
                count = cursor.execute(sql, args_data)
        except pymysql.Error as e:
            self.conn.rollback()
            aelog.exception(e)
        except Exception as e:
            self.conn.rollback()
            aelog.exception(e)
        else:
            self.conn.commit()
        return count

    def find_one(self, sql, args=None):
        """
            查询单条记录
        Args:
            sql: sql 语句
            args: 查询参数
        Returns:
            返回单条记录的返回值
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
        except pymysql.Error as e:
            aelog.exception(e)
        else:
            return cursor.fetchone()

    def find_data(self, sql, args=None, size=None):
        """
            查询指定行数的数据
        Args:
            sql: sql 语句
            args: 查询参数
            size: 返回记录的条数
        Returns:
            返回包含指定行数数据的列表,或者所有行数数据的列表
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
        except pymysql.Error as e:
            aelog.exception(e)
        else:
            return cursor.fetchall() if not size else cursor.fetchmany(size)
