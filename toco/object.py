#!/usr/bin/env python3

from botocore.exceptions import *
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeSerializer
import boto3
import copy
import inspect
import json
import logging
import os

VERSION_KEY = 'version_toco_'

RELATION_SUFFIX = '_rel_toco_'
FKEY_PREFIX = 'toco_fkey='

logger = logging.getLogger(__name__)

def get_class(clazzname):
    '''
    Dynamically retrieve a class.

    From http://stackoverflow.com/questions/547829/how-to-dynamically-load-a-python-class
    '''
    components = clazzname.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def load_object(key, clazzname, recurse=0):
    return get_class(clazzname)(recurse=0, **key)

def is_foreign_key(fkey):
    if not isinstance(fkey, str) or not fkey.startswith(FKEY_PREFIX):
        return False
    try:
        obj = json.loads(fkey[len(FKEY_PREFIX):])
        obj['class']
        obj['key']
        return True
    except:
        # If the above throws an exception, we know it isn't a valid foreign key
        # We might want to raise an exception instead if it has the fkey prefix but isn't valid,
        # but I'll add that later if it looks useful.
        return False

def load_from_fkey(fkey):
    if not is_foreign_key(fkey):
        return None
    obj = json.loads(fkey[len(FKEY_PREFIX):])
    return load_object(key=obj['key'], clazzname=obj['class'])

class Object:
    '''
    Base class for all DynamoDB-storable toco objects.  Cannot itself be instantiated.

    The only thing that a subclass is required to implement is the classmethod SCHEMA, and it must return a dict that can be passed to client.create_table(**schema) and succeed.
    '''
    # Convention: all attributes that start with '_' will not be saved.  Attributes that end with '_toco_' are internal to the system and not for direct use by users.
    _STAGE = os.environ.get('TOCO_STAGE')
    _APP = os.environ.get('TOCO_APP')
    try:
        from django.conf import settings
        _STAGE = settings.TOCO_STAGE
    except BaseException as e:
        pass
    try:
        from django.conf import settings
        _APP = settings.TOCO_APP
    except BaseException as e:
        pass

    def __init__(self, load_depth=1, **kwargs):
        '''

        '''
        self._client = boto3.client('dynamodb')
        self._get_or_create_table()

        self.__dict__[VERSION_KEY] = 0
        description = self._table.get_item(Key=self._extract_hash_and_range(kwargs))
        self.in_db = False
        if description.get('Item'):
            self.__dict__.update(description['Item'])
            self.in_db = True
        self.__dict__.update(kwargs)
        self.unroll_foreign_keys(load_depth=load_depth)

    def unroll_foreign_keys(self, load_depth=1):
        if load_depth > 0:
            fkeys = [key for key in self.__dict__.keys() if is_foreign_key(getattr(self, key))]
            for key in fkeys:
                fkey = getattr(self, key)
                # load the object and also recurse
                setattr(self, key, load_from_fkey(fkey).unroll_foreign_keys(load_depth=load_depth-1))
        # return self so that it can be chained constructor->unroll within a setter
        return self

    @property
    def clazz(self):
        return self.__module__ + "." + self.__class__.__name__

    def get_relation_map(self):
        return {'class':self.clazz,'key':self._extract_hash_and_range()}

    @property
    def _foreign_key(self):
        return FKEY_PREFIX+json.dumps(self.get_relation_map(), sort_keys=True, separators=(',', ':'))

    @classmethod
    def TABLE_NAME(cls):
        '''
        Returns the name of table for the object by pulling it from cls.SCHEMA.  Includes stage and app name, if available from settings or environment variables.
        '''
        schema = cls.SCHEMA()
        TableName = schema.get('TableName')
        if cls._APP:
            TableName = TableName + '_' + str(cls._APP)
        if cls._STAGE:
            TableName = TableName + '_' + str(cls._STAGE)
        return TableName

    def _get_or_create_table(self, stage=None):
        '''
        Create the table needed to store the objects, if it doesn't yet exist.  Either way, return the table.
        '''
        schema = self.SCHEMA()
        schema['TableName'] = self.TABLE_NAME()
        try:
            description = self._client.describe_table(TableName=self.TABLE_NAME())
        except ClientError as e:
            logging.exception()
            self._client.create_table(**schema)
        self._table = boto3.resource('dynamodb').Table(self.TABLE_NAME())

    @classmethod
    def SCHEMA(self):
        '''
        Returns the full table schema needed to create it in DynamoDB, as a dict.
        '''
        raise NotImplementedError("Each subclass must implement this on their own.")

    def _get_dict(self):
        '''
        Parses self.__dict__ and returns only those objects that should be stored in the database.
        '''
        ts = TypeSerializer()
        d = {}
        for a in dir(self):
            try:
                if not inspect.ismethod(getattr(self,a)) and not inspect.isfunction(getattr(self,a)) and not a[0] == '_' and not (hasattr(type(self),a) and isinstance(getattr(type(self),a), property)):
                    if not isinstance(getattr(self,a), Object):
                        # if DDB will choke on the data type, this will throw an error and prevent it from getting added to the dict
                        # however, we convert Objects to foreign-key references before saving, so don't do this if it's one of ours.
                        ts.serialize(getattr(self,a))
                    d[a] = getattr(self,a)
            except Exception as e:
                pass
#                 logger.exception("Exception occured while parsing attr {} of object {}.  NOT STORING.".format(str(a), str(self)))
        return d
# one-line version that sadly doesn't work anymore due to a weird django attribute getting added to objects
#         return {a:getattr(self,a) for a in dir(self) if not inspect.ismethod(getattr(self,a)) and not inspect.isfunction(getattr(self,a)) and not a[0] == '_' and not (hasattr(type(self),a) and isinstance(getattr(type(self),a), property))}



    @property
    def _hash(self):
        return self.__dict__.get(self._get_hash_and_range_keys()[0])

    @property
    def _range(self):
        return self.__dict__.get(self._get_hash_and_range_keys()[1])

    def _get_hash_and_range_keys(self):
        '''
        Returns the hash and range key names (not their values).
        '''
        schema = self.SCHEMA()
        hash = [h['AttributeName'] for h in schema['KeySchema'] if h['KeyType']=='HASH'][0]
        ranges = [r['AttributeName'] for r in schema['KeySchema'] if r['KeyType']=='RANGE']
        range = ranges[0] if ranges else None
        return hash, range

    def _extract_hash_and_range(self, dictionary=None):
        '''
        Returns the hash and range values as a dictionary ready to be passed to client.get_item().
        '''
        if not dictionary:
            dictionary = self.__dict__
        hash, range = self._get_hash_and_range_keys()
        keys = {}
        for k in (hash, range):
            if k and k in dictionary.keys():
                keys[k] = dictionary[k]
        return keys

    def save(self, force=False):
        '''
        Saves the item, whether or not it already exists.

        If force=False (the default) and another location has modified the object since this copy was loaded, it will fail.
        If force=True, it will blow away whatever's in the entry and replace it with this copy.
        '''
        old_version = self.__dict__.get(VERSION_KEY)
        if not force:
            CE = Attr(VERSION_KEY).eq(old_version)
        try:
            self.__dict__[VERSION_KEY] = old_version+1
            self.in_db = True
            if force:
                self._store()
            else:
                self._store(CE)
        except ClientError as e:
            print('Update failed: ' + str(e))
            self.__dict__[VERSION_KEY] = old_version
            raise e

    def update(self, force=False):
        '''
        Saves the item, but only if it already exists in the table.

        If force=False (the default) and another location has modified the object since this copy was loaded, it will fail.
        If force=True, it will blow away whatever's in the entry and replace it with this copy.
        '''
        old_version = self.__dict__.get(VERSION_KEY)
        hash,range = self._get_hash_and_range_keys()
        CE = ConditionExpression=Attr(hash).eq(getattr(self,hash)) & Attr(range).eq(getattr(self,range)) if range else Attr(hash).eq(getattr(self,hash))
        if not force:
            CE = CE & Attr(VERSION_KEY).eq(old_version)
        try:
            self.in_db = True
            self.__dict__[VERSION_KEY] = old_version+1
            self._store(CE)
        except ClientError as e:
            print('Update failed: ' + str(e))
            self.__dict__[VERSION_KEY] = old_version
            raise e

    def create(self):
        '''
        Saves the item, but only if it isn't yet in the table.
        '''
        hash,range = self._get_hash_and_range_keys()
        CE = ConditionExpression=Attr(hash).ne(getattr(self,hash)) & Attr(range).ne(getattr(self,range)) if range else Attr(hash).ne(getattr(self,hash))
        self.in_db = True
        self._store(CE)

    def _store(self, CE=None):
        # Broken out separately so that we can easily manipulate the blob being saved.
        dict_to_save = copy.copy(self._get_dict())
        for key in [k for k in self._get_dict().keys() if isinstance(self._get_dict()[k], Object)]:
            fkey = self._get_dict()[key]._foreign_key
            dict_to_save[key] = fkey
        if CE:
            self._table.put_item(Item=dict_to_save, ConditionExpression=CE)
        else:
            self._table.put_item(Item=dict_to_save)

    def reload(self):
        '''
        Reloads the item's attributes from DynamoDB, replacing whatever's currently in the object.
        '''
        description = self._table.get_item(Key=self._extract_hash_and_range(self.__dict__))
        if description.get('Item'):
            self.__dict__.update(description['Item'])
            self.unroll_foreign_keys(load_depth=1)
