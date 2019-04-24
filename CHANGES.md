## aclients Changelog

###[1.0.0b34] - 2019-4-24

#### Added 
- 修改aiohttp client增加针对ip地址的URL中session接受cookie的功能开关

###[1.0.0b33] - 2019-4-23

#### Added 
- 工具类utils中增加用于枚举实例的元类
- tinylibs包中增加简单的异步信号实现blinker模块

#### Changed 
- 移动单例类Singleton从decorators到utils工具类

###[1.0.0b32] - 2019-4-22

#### Changed 
- 修改schema message装饰器判断错误的情况


###[1.0.0b31] - 2019-4-21

#### Changed 
- 修改sanic版本为长期支持版本18.12LTS

###[1.0.0b30] - 2019-4-20

#### Changed 
- 优化redis客户端，修改出现的错误

###[1.0.0b29] - 2019-4-19

#### Changed 
- 修改redis中获取session返回session对象出错的问题

###[1.0.0b28] - 2019-4-19

#### Added 
- 增加保存和更新hash数据时对单个键值进行保存和更新的功能

###[1.0.0b27] - 2019-4-19

#### Changed 
- 修改获取redis数据时可能出现的没有把字符串转换为对象的情况
- 修改保存redis数据时指定是否进行dump以便进行性能的提高
- 修改获取redis数据时指定是否进行load以便进行性能的提高


###[1.0.0b26] - 2019-4-18

#### Changed 
- 修改Session中增加page_id和page_menu_id两个用于账户的页面权限管理

###[1.0.0b25] - 2019-4-16

#### Changed 
- 修改TinyMySQL中execute的参数，修改find中的args参数名称

###[1.0.0b24] - 2019-4-2

#### Added 
- 工具类中增加由对象名生成类名的功能
- 工具类中增加解析yaml文件的功能
- 工具类中增加返回objectid的功能
- 增加基于pymysql的简单TinyMySQL功能，用于简单操作MySQL的时候使用 

###[1.0.0b22] - 2019-3-25

#### Changed 
- 修改update data中设置前缀的错误

###[1.0.0b20] - 2019-3-22

#### Changed 
- 修改mongo插入中的错误，修改id时的错误

###[1.0.0b19] - 2019-3-22

#### Changed 
- 修改mongo查询中的错误

###[1.0.0b18] - 2019-3-21

#### Added 
- redis的session中增加角色ID
- redis的session中增加静态权限ID
- redis的session中增加动态权限ID

#### Changed 
- redis的session中user_id更改为account_id

###[1.0.0b17] - 2019-3-21

#### Added 
- 增加同步方法包装为异步方法的功能 

###[1.0.0b16] - 2019-3-16

#### Changed 
- 优化insert document中的ID处理逻辑 
- 优化update data处理逻辑 
- 优化query key处理逻辑，可以直接使用in、like等查询

###[1.0.0b15] - 2019-1-31

#### Changed 
- 修改schema装饰器实现

###[1.0.0b14] - 2019-1-31

#### Added 
- 增加元类单例实现

#### Changed 
- 修改httpclient为元类单例的子类

###[1.0.0b13] - 2019-1-28

#### Changed 
- 优化MySQL或查询支持列表

###[1.0.0b12] - 2019-1-28

#### Changed 
- 删除http message 中不用的消息
- 优化exceptions的实现方式

###[1.0.0b11] - 2019-1-25

#### Changed 
- 修改schema_validate装饰器能够修改提示消息的功能,如果多个地方用到此装饰器，在其中一处修改即可

###[1.0.0b10] - 2019-1-25

#### Added 
- 添加schema_validate装饰器用于校验schema
#### Changed 
- 修改http message默认值

###[1.0.0b9] - 2019-1-21

#### Added 
- 增加多数据库、多实例应用方式
- 增加http client的测试
- 增加在没有app时脚本中使用时的初始化功能,这样便于通用性
- 增加错误类型，能够对错误进行定制
- 增加单例的装饰器，修改httpclient为单例
#### Changed 
- 修改一处可能引起错误的地方

###[1.0.0b8] - 2019-1-18

#### Changed 
- 修改初始化方式，更改为sanic扩展的初始化方式，即init_app
- 修改初始化时配置的加载顺序，默认先加载

###[1.0.0b7] - 2019-1-18

#### Changed 
- 从b2到b7版本的修改记录忘记了，这里先不记录了

###[1.0.0b1] - 2018-12-26

#### Added 

- MySQL基于aiomysql和sqlalchemy的CRUD封装
- http基于aiohttp的CRUD封装
- session基于aredis的CRUD封装
- redis基于aredis的常用封装
- mongo基于motor的CRUD封装
- 所有消息可自定义配置,否则为默认配置
