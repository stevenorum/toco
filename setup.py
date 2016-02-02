#!/usr/bin/env python3

from distutils.core import setup

setup(name='toco',
      version='0.1.0',
      description='DynamoDB-based user and resource management.',
      author='Steve Norum',
      author_email='stevenorum@gmail.com',
      url='www.stevenorum.com',
      packages=['toco','toco.django'],
#       package_dir={'toco': 'toco'},
#       test_suite='tests',
)
