#!/usr/bin/env python3

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import boto3
import inspect

VERSION_KEY = 'version_toco_'

class Object:
    # Convention: all attributes that start with '_' will not be saved.  Attributes that end with '_toco_' are internal to the system and not for direct use by users.

    def __init__(self, **kwargs):
        self._client = boto3.client('dynamodb')
        self.get_or_create_table()

        self.__dict__[VERSION_KEY] = 0
        description = self._table.get_item(Key=self._extract_hash_and_range(kwargs))
        if description.get('Item'):
            self.__dict__.update(description['Item'])
        self.__dict__.update(kwargs)

    def add_attrs(self, *args, **kwargs):
        for arg in args:
            if isinstance(arg, dict):
                self.__dict__.update(arg)
            elif isinstance(arg, (list, tuple)):
                for element in arg:
                    self.__dict__.update(element)
        self.__dict__.update(kwds)

    def get_or_create_table(self):
        schema = self.get_schema()
        try:
            description = self._client.describe_table(TableName=schema.get('TableName'))
        except ClientError as e:
            self._client.create_table(**schema)
        self._table = boto3.resource('dynamodb').Table(schema.get('TableName'))

    def get_schema(self):
        raise NotImplementedError("Each subclass must implement this on their own.")

    def _get_dict(self):
        return {a:getattr(self,a) for a in dir(self) if not inspect.ismethod(getattr(self,a)) and not inspect.isfunction(getattr(self,a)) and not a[0] == '_'}

    def _get_hash_and_range_keys(self):
        schema = self.get_schema()
        hash = [h['AttributeName'] for h in schema['KeySchema'] if h['KeyType']=='HASH'][0]
        ranges = [r['AttributeName'] for r in schema['KeySchema'] if r['KeyType']=='RANGE']
        range = ranges[0] if ranges else None
        return hash, range

    def _extract_hash_and_range(self, dictionary):
        hash, range = self._get_hash_and_range_keys()
        keys = {}
        for k in (hash, range):
            if k and k in dictionary.keys():
                keys[k] = dictionary[k]
        return keys

    def save(self, force=False):
        old_version = self.__dict__.get(VERSION_KEY)
        if not force:
            CE = Attr(VERSION_KEY).eq(old_version)
        try:
            self.__dict__[VERSION_KEY] = old_version+1
            if force:
                self._table.put_item(Item=self._get_dict())
            else:
                self._table.put_item(Item=self._get_dict(), ConditionExpression=CE)
        except ClientError as e:
            print('Update failed: ' + str(e))
            self.__dict__[VERSION_KEY] = old_version
            raise e

    def update(self, force=False):
        old_version = self.__dict__.get(VERSION_KEY)
        hash,range = self._get_hash_and_range_keys()
        CE = ConditionExpression=Attr(hash).eq(getattr(self,hash)) & Attr(range).eq(getattr(self,range)) if range else Attr(hash).eq(getattr(self,hash))
        if not force:
            CE = CE & Attr(VERSION_KEY).eq(old_version)
        try:
            self.__dict__[VERSION_KEY] = old_version+1
            self._table.put_item(Item=self._get_dict(), ConditionExpression=CE)
        except ClientError as e:
            print('Update failed: ' + str(e))
            self.__dict__[VERSION_KEY] = old_version
            raise e

    def create(self):
        hash,range = self._get_hash_and_range_keys()
        CE = ConditionExpression=Attr(hash).ne(getattr(self,hash)) & Attr(range).ne(getattr(self,range)) if range else Attr(hash).ne(getattr(self,hash))
        self._table.put_item(Item=self._get_dict(), ConditionExpression=CE)

    def reload(self):
        description = self._table.get_item(Key=self._extract_hash_and_range(self.__dict__))
        if description.get('Item'):
            self.__dict__.update(description['Item'])
