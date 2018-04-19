#!/usr/bin/env python3

from distutils.core import setup

MAJOR_VERSION='0'
MINOR_VERSION='0'
PATCH_VERSION='3'

VERSION = "{}.{}.{}".format(MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION)

def main():
    setup(
        name = 'toco',
        packages = ['toco'],
        version = VERSION,
        description = 'Basic tools for interacting with DynamoDB.',
        author = 'Steve Norum',
        author_email = 'sn@drunkenrobotlabs.org',
        url = 'https://github.com/stevenorum/toco',
        download_url = 'https://github.com/stevenorum/toco/archive/{}.tar.gz'.format(VERSION),
        keywords = ['python','aws','dynamodb'],
        classifiers = [],
    )

if __name__ == "__main__":
    main()
