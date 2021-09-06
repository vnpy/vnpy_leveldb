# vn.py框架的LevelDB数据库管理器

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-linux|windows-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
</p>

## 说明

基于plyvel开发的LevelDB数据库接口，需要首先安装leveldb数据库才能安装使用。

## 安装

安装需要基于2.4.0版本以上的[VN Studio](https://www.vnpy.com)。

安装需要基于leveldb数据库

下载解压后在cmd运行：

```
python setup.py install
```

## 性能

对比sqlite对22755条数据进行连续进行100次读写操作

LevelDB

平均存储时间： 0.7550472712516785s

平均读取时间 1.6066341471672059s

获取overview平均时间 0.0003154873847961426s

平均删除时间 0.22815822601318358s

Sqlite

平均存储消耗时间： 1.8237190175056457s

平均读取消耗时间 2.17199054479599s

获取一条overview平均消耗时间 0.0025047659873962402s

平均删除消耗时间 0.07287977933883667s