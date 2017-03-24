#!/usr/bin/env python

from setuptools import setup
setup(
    name='multicastclient',
    version='1.0a',
    description='Bus utils for pyDip',
    author='IoT Superheroes',
    author_email='mikael.m.magnusson@gmail.com',
    packages=[ 'multicastclient'   ],
    requires=[ 'pyloggedthread'
    ],
      install_requires=[
            'ConcurrentLogHandler'
      ])
