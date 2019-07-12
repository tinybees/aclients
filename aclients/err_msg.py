#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午2:42
可配置消息模块
"""

__all__ = ("mysql_msg", "mongo_msg", "http_msg", "schema_msg")

# mysql 从1到100
mysql_msg = {
    1: {"msg_code": 1, "msg_zh": "MySQL插入数据失败.", "msg_en": "MySQL insert data failed.",
        "description": "MySQL插入数据时最终失败的提示"},
    2: {"msg_code": 2, "msg_zh": "MySQL更新数据失败.", "msg_en": "MySQL update data failed.",
        "description": "MySQL更新数据时最终失败的提示"},
    3: {"msg_code": 3, "msg_zh": "MySQL删除数据失败.", "msg_en": "MySQL delete data failed.",
        "description": "MySQL删除数据时最终失败的提示"},
    4: {"msg_code": 4, "msg_zh": "MySQL查找单条数据失败.", "msg_en": "MySQL find one data failed.",
        "description": "MySQL查找单条数据时最终失败的提示"},
    5: {"msg_code": 5, "msg_zh": "MySQL查找多条数据失败.", "msg_en": "MySQL find many data failed.",
        "description": "MySQL查找多条数据时最终失败的提示"},
    6: {"msg_code": 6, "msg_zh": "MySQL执行SQL失败.", "msg_en": "MySQL execute sql failed.",
        "description": "MySQL执行SQL失败的提示"},
}

# mongo 从100到200
mongo_msg = {
    100: {"msg_code": 100, "msg_zh": "MongoDB插入数据失败.", "msg_en": "MongoDB insert data failed.",
          "description": "MongoDB插入数据时最终失败的提示"},
    101: {"msg_code": 101, "msg_zh": "MongoDB更新数据失败.", "msg_en": "MongoDB update data failed.",
          "description": "MongoDB更新数据时最终失败的提示"},
    102: {"msg_code": 102, "msg_zh": "MongoDB删除数据失败.", "msg_en": "MongoDB delete data failed.",
          "description": "MongoDB删除数据时最终失败的提示"},
    103: {"msg_code": 103, "msg_zh": "MongoDB查找单条数据失败.", "msg_en": "MongoDB find one data failed.",
          "description": "MongoDB查找单条数据时最终失败的提示"},
    104: {"msg_code": 104, "msg_zh": "MongoDB查找多条数据失败.", "msg_en": "MongoDB find many data failed.",
          "description": "MongoDB查找多条数据时最终失败的提示"},
    105: {"msg_code": 105, "msg_zh": "MongoDB聚合查询数据失败.", "msg_en": "MongoDB aggregate query data failed.",
          "description": "MongoDB聚合查询数据时最终失败的提示"},
}

# request and schema 从200到300
http_msg = {
    200: {"msg_code": 200, "msg_zh": "获取API响应结果失败.", "msg_en": "Failed to get API response result.",
          "description": "async request 获取API响应结果失败时的提示"},
}

schema_msg = {
    # schema valication message
    201: {"msg_code": 201, "msg_zh": "数据提交有误，请重新检查.", "msg_en": "Request body validation error, please check!",
          "description": "marmallow校验body错误时的提示"},
    202: {"msg_code": 202, "msg_zh": "数据提交未知错误，请重新检查.",
          "msg_en": "Request body validation unknow error, please check!",
          "description": "marmallow校验body未知错误时的提示"},
}
