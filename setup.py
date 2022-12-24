from setuptools import setup
from setuptools import find_packages

setup(
    name='kvdbpy',
    version='1.0.0',
    description='Implementation of KV db. Keys are stored in-memory hash table.',
    author='wh0ois',
    packages=find_packages(exclude=('tests*'))
)
