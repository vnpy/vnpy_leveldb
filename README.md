# vn.py框架的LevelDB数据库接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
</p>

## 说明

基于plyvel-win32开发的LevelDB数据库接口。

## 使用

在vn.py中使用MongoDB时，需要在全局配置中填写以下字段信息：

|名称|含义|必填|举例|
|---------|----|---|---|
|database.name|名称|是|mongodb|
|database.host|地址|是|localhost|
|database.port|端口|是|27017|
|database.database|实例|是|vnpy|
|database.user|用户名|否||
|database.password|密码|否||