#!/usr/bin/env python3

from setuptools import setup

setup(name='toco',
      version='0.1.0',
      description='DynamoDB-based user and resource management.',
      author='Steve Norum',
      author_email='stevenorum@gmail.com',
      url='www.stevenorum.com',
      packages=['toco'],
      package_dir={'toco': 'toco'},
      test_suite='tests',
)
