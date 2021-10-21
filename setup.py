from setuptools import setup
import platform

if platform.system() == "Windows":
    setup(install_requires = "plyvel-win32")
elif platform.system() == "Linux":
    setup(install_requires = "plyvel")
