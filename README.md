# VeighNa框架的LevelDB数据库接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.1-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macox-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7|3.8|3.9|3.10-blue.svg" />
</p>

## 说明

基于plyvel开发的LevelDB数据库接口。

对于不同操作系统需要安装不通版本的plyvel。对于linux与windows会自动安装适配的plyvel与plyvel-win32。

对于macox操作系统，需要手动下载源码进行安装，并在安装前将源码setup.py中

```python
if platform.system() == 'Darwin':
    extra_compile_args += ['-stdlib=libc++']
```

修改为

```python
if platform.system() == 'Darwin':
    extra_compile_args = ['-Wall', '-g', '-x', 'c++', '-std=c++11', '-fno-rtti']
```

再运行

```bash
python -m pip install .
```

进行手动安装。方法参考自[https://github.com/wbolster/plyvel/issues/114]。

## 使用

在VeighNa中使用LevelDB时，需要在全局配置中填写以下字段信息：

|名称|含义|必填|举例|
|---------|----|---|---|
|database.name|名称|是|leveldb|
|database.database|实例|是|vnpy_data|
