#!/usr/bin/env python3

from setuptools import setup
import tenzing

setup(name='toco',
      version='0.1.0',
      description='DynamoDB data relationship management framework.',
      author='Steve Norum',
      author_email='stevenorum@gmail.com',
      url='www.stevenorum.com',
      packages=['toco'],
      package_dir={'toco': 'toco'}, 
      cmdclass = {'upload':tenzing.Upload}
)
