# coding=utf-8

"""
MIT License

Copyright (c) 2018 Tiny Bees

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""
from setuptools import setup

from aclients import __version__

setup(name='aclients',
      version=__version__,
      description='基础封装库',
      long_description=open('README.md').read(),
      long_description_content_type='text/markdown',
      author='TinyBees',
      author_email='a598824322@qq.com',
      url='https://github.com/tinybees/aclients',
      packages=['aclients', 'aclients.tinylibs'],
      entry_points={},
      requires=['sanic', 'aelog', 'aredis', 'hiredis', 'aiomysql', 'sqlalchemy', 'aiohttp', 'cchardet', 'aiodns',
                'motor', 'ujson', 'marshmallow', 'PyYAML'],
      install_requires=['sanic==18.12',
                        'aelog>=1.0.3',
                        'aredis>=1.1.3',
                        'hiredis',
                        'aiomysql>=0.0.19',
                        'sqlalchemy>=1.2.12',
                        'aiohttp>=3.3.2',
                        'cchardet',
                        'aiodns',
                        'motor>=1.2.2',
                        'ujson',
                        'marshmallow>=3.0.0rc3',
                        'PyYAML>=3.13'],
      python_requires=">=3.5",
      keywords="mysql, mongo, redis, http, asyncio, crud, session",
      license='MIT',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Natural Language :: Chinese (Simplified)',
          'Operating System :: POSIX :: Linux',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: MacOS :: MacOS X',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: Utilities',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7']
      )
