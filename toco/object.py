#!/usr/bin/env python3

from botocore.exceptions import *
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeSerializer
import boto3
import inspect
import logging

VERSION_KEY = 'version_toco_'

logger = logging.getLogger(__name__)

class Object:
    # Convention: all attributes that start with '_' will not be saved.  Attributes that end with '_toco_' are internal to the system and not for direct use by users.
    _STAGE = None
    try:
        from django.conf import settings
        _STAGE = settings.TOCO_STAGE
    except BaseException as e:
        pass

    def __init__(self, **kwargs):
        self._client = boto3.client('dynamodb')
        self.get_or_create_table()

        self.__dict__[VERSION_KEY] = 0
        description = self._table.get_item(Key=self._extract_hash_and_range(kwargs))
        self.in_db = False
        if description.get('Item'):
            self.__dict__.update(description['Item'])
            self.in_db = True
        self.__dict__.update(kwargs)

    def add_attrs(self, *args, **kwargs):
        for arg in args:
            if isinstance(arg, dict):
                self.__dict__.update(arg)
            elif isinstance(arg, (list, tuple)):
                for element in arg:
                    self.__dict__.update(element)
        self.__dict__.update(kwds)

    @classmethod
    def TABLE_NAME(cls):
        schema = cls.get_schema()
        TableName = schema.get('TableName')
        if cls._STAGE:
            TableName = TableName + '_' + str(cls._STAGE)
        return TableName

    def get_or_create_table(self, stage=None):
        schema = self.get_schema()
        schema['TableName'] = self.TABLE_NAME()
        try:
            description = self._client.describe_table(TableName=self.TABLE_NAME())
        except ClientError as e:
            logging.exception()
            self._client.create_table(**schema)
        self._table = boto3.resource('dynamodb').Table(self.TABLE_NAME())

    def get_schema(self):
        raise NotImplementedError("Each subclass must implement this on their own.")

    def _get_dict(self):
        ts = TypeSerializer()
        d = {}
        for a in dir(self):
            try:
                if not inspect.ismethod(getattr(self,a)) and not inspect.isfunction(getattr(self,a)) and not a[0] == '_' and not (hasattr(type(self),a) and isinstance(getattr(type(self),a), property)):
                    ts.serialize(getattr(self,a)) # if DDB will choke on the data type, this will throw an error and prevent it from getting added to the dict
                    d[a] = getattr(self,a)
            except Exception as e:
                logger.exception("Exception occured while parsing attr {} of object {}.  NOT STORING.".format(str(a), str(self)))
        return d
# one-line version that sadly doesn't work anymore due to a weird django attribute getting added to objects
#         return {a:getattr(self,a) for a in dir(self) if not inspect.ismethod(getattr(self,a)) and not inspect.isfunction(getattr(self,a)) and not a[0] == '_' and not (hasattr(type(self),a) and isinstance(getattr(type(self),a), property))}

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
            self.in_db = True
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
            self.in_db = True
            self.__dict__[VERSION_KEY] = old_version+1
            self._table.put_item(Item=self._get_dict(), ConditionExpression=CE)
        except ClientError as e:
            print('Update failed: ' + str(e))
            self.__dict__[VERSION_KEY] = old_version
            raise e

    def create(self):
        hash,range = self._get_hash_and_range_keys()
        CE = ConditionExpression=Attr(hash).ne(getattr(self,hash)) & Attr(range).ne(getattr(self,range)) if range else Attr(hash).ne(getattr(self,hash))
        self.in_db = True
        self._table.put_item(Item=self._get_dict(), ConditionExpression=CE)

    def reload(self):
        description = self._table.get_item(Key=self._extract_hash_and_range(self.__dict__))
        if description.get('Item'):
            self.__dict__.update(description['Item'])
