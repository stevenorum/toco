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

def load_object_from_relation(relation, recurse=0):
    return load_object(key=relation['key'], clazzname=relation['class'], recurse=recurse)

def fkey_from_relation(relation):
    keys = ["{}:{}".format(key, relation['key'][key]) for key in relation['key']]
    keys += ['_class:'+str(relation['class'])]
    return '/toco/'.join(sorted(keys)) # wierd combiner, I realize, but it'll help avoid collisions with user keys

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

    def __init__(self, recurse=0, **kwargs):
        '''

        '''
        self._client = boto3.client('dynamodb')
        self._get_or_create_table()

        self.__dict__[VERSION_KEY] = 0
        description = self._table.get_item(Key=self._extract_hash_and_range(kwargs))
        self.in_db = False
        if description.get('Item'):
            self.__dict__.update(description['Item'])
            for k in [key for key in description['Item'].keys() if key.endswith(RELATION_SUFFIX)]:
#                 self.__dict__[k] = json.loads(description['Item'][k])
                self.__dict__[k] = description['Item'][k]
            self.in_db = True
        self.__dict__.update(kwargs)
        if recurse > 0:
#             print("Recurse={}, recursing...".format(str(recurse)))
            self.add_relations(recurse=recurse-1)

    @property
    def clazz(self):
        return self.__module__ + "." + self.__class__.__name__

    @property
    def _foreign_key(self):
        return fkey_from_relation({'class':self.clazz,'key':self._extract_hash_and_range()})

    def load_relations(self, recurse=0):
        '''
        Searches the object's attributes for relation info and when found, loads the related objects.
        '''
        keys = self.__dict__.keys()
        relation_keys = [key for key in self.__dict__.keys() if key.endswith(RELATION_SUFFIX)]
        for key in relation_keys:
#             print("Relation key: "+key)
            relation = getattr(self, key)
#             print(relation)
            obj_name = key[:-len(RELATION_SUFFIX)]
            if hasattr(self, obj_name) and isinstance(getattr(self, obj_name, 3), Object):
#                 print("Already has a value for this relation: " + str(getattr(self, obj_name, None)))
                pass
            else:
                setattr(self, obj_name, load_object_from_relation(relation=relation, recurse=recurse))
#             print("Attr set: "+str(getattr(self, obj_name)))

    def add_relations(self, recurse=0):
        '''
        Adds the detailed relation entry for each Object that's an attribute of this Object.

        This should be called before every save and after every load.  (The code currently already does this.)
        This should be called after loading an object.
        However, this SHOULD NOT currently be added to a constructor.  If a bidirectional relationship exists, that could cause an infinite loop.
        '''
        self.relate(only_objects=True, **(self.__dict__))
        self.load_relations(recurse=recurse)

    def relate(self, only_objects=True, **kwargs):
        '''
        Lets you set multiple attributes at once.  If any of them are Object subclasses, automatically parses out their hash and range keys.
        STILL A WORK IN PROGRESS
        '''
        print("Params to relate:")
        print(str(kwargs))
#         to_add = copy.copy(kwargs)
        to_add = {}
        for key in kwargs:
#             print("Dict key: "+key)
            obj = kwargs[key]
#             print(obj)
            if isinstance(obj, Object):
#                 print("Is an object!")
                relation = {'key':obj._extract_hash_and_range(), 'class':obj.clazz}
                to_add[str(key)] = obj
                to_add[str(key) + RELATION_SUFFIX] = relation
        print("Relate is about to update with the following dict:")
        print(str(to_add))
        self.__dict__.update(to_add)

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
                    ts.serialize(getattr(self,a)) # if DDB will choke on the data type, this will throw an error and prevent it from getting added to the dict
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
            self.add_relations()
            self.in_db = True
            if force:
                self._store()
#                 self._table.put_item(Item=self._get_dict())
            else:
                self._store(CE)
#                 self._table.put_item(Item=self._get_dict(), ConditionExpression=CE)
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
            self.add_relations()
            self.in_db = True
            self.__dict__[VERSION_KEY] = old_version+1
            self._store(CE)
#             self._table.put_item(Item=self._get_dict(), ConditionExpression=CE)
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
        self.add_relations()
        self.in_db = True
        self._store(CE)
#         self._table.put_item(Item=self._get_dict(), ConditionExpression=CE)

    def _store(self, CE=None):
        print("About to store object "+str(self))
        print("Full contents: {}".format(self.__dict__))
        dict_to_save = self._get_dict()
        for relation in [k for k in self._get_dict().keys() if k.endswith(RELATION_SUFFIX)]:
            dict_to_save[relation[:-1*len(RELATION_SUFFIX)]] = fkey_from_relation(self._get_dict()[relation])
        print("Contents to store: {}".format(dict_to_save))
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
            self.add_relations()
