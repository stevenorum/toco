#!/usr/bin/env python3A

from botocore.exceptions import *
from boto3.dynamodb.conditions import Key, Attr, Or
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
    Dynamically retrieve a class from its name.

    (From http://stackoverflow.com/questions/547829/how-to-dynamically-load-a-python-class)

    :param clazzname: Name of the class to load.
    :rtype: Class
    '''
    components = clazzname.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def load_object(clazzname, recurse=0, **kwargs):
    '''
    Load the object with the given key and the given class.

    :param key: Dict containing the DynamoDB hash and, if applicable, range keys.  The key names should be the dict keys, and the key values should be the values.
    :param clazzname: Name of the class of the object.
    :param recurse: How deeply to load objects.  0 (default) just loads the given object.  If it has as attributes any toco Objects, they are not loaded.  Passing 1 will load those objects from DynamoDB.  Passing 2 will load any toco Object attributes of those objects, and so on.
    :rtype: toco object
    '''
    return get_class(clazzname)(recurse=0, **kwargs)

def is_foreign_key(fkey):
    '''
    Determines whether a given object is a toco foreign key, or a string containing a classname and the keys necessary to load an object from DynamoDB.

    :param fkey: An object that may or may not be a toco foreign key.
    :rtype: toco object
    '''
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
    '''
    If fkey is a toco foreign key, load it from DynamoDB.

    :param fkey: A toco foreign key.
    :rtype: toco object
    '''
    if not is_foreign_key(fkey):
        return None
    obj = json.loads(fkey[len(FKEY_PREFIX):])
    key = obj['key']
    clazzname = obj['class']
    del obj['key']
    del obj['class']
    obj.update(key)
    return load_object(clazzname=clazzname, **obj)

class Object(object):
    '''
    Base class for all DynamoDB-storable toco objects.  Cannot itself be instantiated.

    The only thing that a subclass is required to implement is the classmethod SCHEMA, and it must return a dict that can be passed to client.create_table(**schema) and succeed.

    Constructor args:

    :param load_depth: How deeply to load objects.  0 just loads the given object.  If it has as attributes any toco Objects, they are not loaded.  Passing 1 (default) will load those objects from DynamoDB.  Passing 2 will load any toco Object attributes of those objects, and so on.
    :param kwargs: Keys for an object, and any attributes to attach to that object.
    :rtype: toco object
    '''
    # Convention: all attributes that start with '_' will not be saved.  Attributes that end with '_toco_' are internal to the system and not for direct use by users.
    _STAGE = os.environ.get('TOCO_STAGE')
    _APP = os.environ.get('TOCO_APP')
    _TABLE = None
    _CLASSNAME = None
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
        self._table = None
        self._client = boto3.client('dynamodb')
        self._get_or_create_table()

        self.__dict__[VERSION_KEY] = 0
        try:
            description = self._table.get_item(Key=self._extract_hash_and_range(kwargs))
        except ClientError as e:
            description = {}
        self.in_db = False
        if description.get('Item'):
            self.__dict__.update(description['Item'])
            self.in_db = True
        self.__dict__.update(kwargs)
        self.unroll_foreign_keys(load_depth=load_depth)

    def unroll_foreign_keys(self, load_depth=1):
        '''
        Recursively loads from DynamoDB any attributes of the object that are foreign keys.

        :param load_depth: How deep to recursively load.
        '''
        if load_depth > 0:
            fkeys = [key for key in self.__dict__.keys() if is_foreign_key(getattr(self, key))]
            for key in fkeys:
                fkey = getattr(self, key)
                # load the object and also recurse
                setattr(self, key, load_from_fkey(fkey).unroll_foreign_keys(load_depth=load_depth-1))
        # return self so that it can be chained constructor->unroll within a setter
        return self

    @staticmethod
    def ensure_loaded(obj):
        if isinstance(obj, Object):
            return obj
        elif is_foreign_key(obj):
            return load_from_fkey(obj)
        else:
            return None

    @classmethod
    def get_classname(cls):
        return cls._CLASSNAME if cls._CLASSNAME else "{module}.{name}".format(module=cls.__module__, name=cls.__name__)

    def get_relation_map(self):
        classes = []
        new_classes = [self.__class__]
        has_method = True
        while new_classes:
            bases = []
            for clazz in new_classes:
                if clazz not in classes and hasattr(clazz, "_get_relation_map"):
                    classes.append(clazz)
                    bases.extend(clazz.__bases__)
            new_classes = bases
        relation_map = {}
        for clazz in classes[::-1]:
            relation_map.update(clazz._get_relation_map(self))
        return relation_map

    @classmethod
    def _get_relation_map(cls, obj):
        return {'class':cls.get_classname(), 'key':obj._extract_hash_and_range()}

    @property
    def _foreign_key(self):
        '''
        The foreign key necessary to load this object from DynamoDB.
        '''
        return FKEY_PREFIX+json.dumps(self.get_relation_map(), sort_keys=True, separators=(',', ':'))

    @classmethod
    def get_required_attributes(cls):
        '''
        Returns all attributes that are required for an object to be saved but that aren't called out in the main schema as hash or range keys.
        '''
        return getattr(cls, 'REQUIRED_ATTRS', [])

    def TABLE_NAME(self):
        '''
        Returns the name of table for the object by pulling it from cls.SCHEMA.  Includes stage and app name, if available from settings or environment variables.
        '''
        schema = self.SCHEMA()
        TableName = schema.get('TableName')
        if self._APP:
            TableName = TableName + '_' + str(self._APP)
        if self._STAGE:
            TableName = TableName + '_' + str(self._STAGE)
        return TableName

    def _get_or_create_table(self, use_cache=True, update_class=False):
        '''
        Create the table needed to store the objects, if it doesn't yet exist.  Either way, return the table.

        :rtype: DynamoDB Table
        '''
        if use_cache:
            if self._table:
                return self._table
            elif self.__class__._TABLE:
                return self.__class__._TABLE
        self._table = self._get_or_create_table_inner()
        if update_class:
            self.__class__._TABLE = self._table
        return self._table

    def _get_or_create_table_inner(self):
        schema = self.SCHEMA()
        schema['TableName'] = self.TABLE_NAME()
        try:
            description = self._client.describe_table(TableName=self.TABLE_NAME())
        except ClientError as e:
            logging.exception('')
            self._client.create_table(**schema)
        return boto3.resource('dynamodb').Table(self.TABLE_NAME())

    def SCHEMA(self):
        '''
        Returns the full table schema needed to create it in DynamoDB, as a dict.
        '''
        raise NotImplementedError("Each subclass must implement this on their own.")

    def _get_dict(self):
        '''
        Parses self.__dict__ and returns only those objects that should be stored in the database.

        :rtype: dict
        '''
        ts = TypeSerializer()
        d = {}
        for a in [k for k in dir(self) if k not in dir(type(self))]:
            # limited to only instance attributes, not class attributes
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
        '''
        Returns the value of the hash key for this object.
        '''
        return self.__dict__.get(self._get_hash_and_range_keys()[0])

    @property
    def _range(self):
        '''
        Returns the value of the range key for this object, or None if there is no range key.
        '''
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

    def create_and_return(self, force=False):
        self.create(force)
        return self

    def update_and_return(self, force=False):
        self.update(force)
        return self

    def save_and_return(self, force=False):
        self.save(force)
        return self

    def save(self, force=False):
        '''
        Saves the item, whether or not it already exists.

        If force=False (the default) and another location has modified the object since this copy was loaded, it will fail.
        If force=True, it will blow away whatever's in the entry and replace it with this copy.
        '''
        old_version = self.__dict__.get(VERSION_KEY)
        if not force:
            CE = Or(Attr(VERSION_KEY).eq(old_version), Attr(VERSION_KEY).not_exists())
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
        return self

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
        return self

    def create(self):
        '''
        Saves the item, but only if it isn't yet in the table.
        '''
        hash,range = self._get_hash_and_range_keys()
        CE = ConditionExpression=Attr(hash).ne(getattr(self,hash)) & Attr(range).ne(getattr(self,range)) if range else Attr(hash).ne(getattr(self,hash))
        self.in_db = True
        self._store(CE)
        return self

    def _store(self, CE=None):
        # Broken out separately so that we can easily manipulate the blob being saved.
        dict_to_save = copy.copy(self._get_dict())
        for key in [k for k in self._get_dict().keys() if isinstance(self._get_dict()[k], Object)]:
            fkey = self._get_dict()[key]._foreign_key
            dict_to_save[key] = fkey
        required = self.get_required_attributes()
        missing = [r for r in required if not r in dict_to_save or not dict_to_save[r]]
        if missing:
            raise RuntimeError('The following attributes are missing and must be added before saving: '+', '.join(missing))
        if CE:
            self._table.put_item(Item=dict_to_save, ConditionExpression=CE)
        else:
            self._table.put_item(Item=dict_to_save)
        return self

    def reload(self):
        '''
        Reloads the item's attributes from DynamoDB, replacing whatever's currently in the object.
        '''
        description = self._table.get_item(Key=self._extract_hash_and_range(self.__dict__))
        if description.get('Item'):
            self.__dict__.update(description['Item'])
            self.unroll_foreign_keys(load_depth=1)
        return self

class CFObject(Object):
    '''
    Base class for toco objects that are based on tables created in a CloudFormation stack.
    '''

    _CF_STACK_NAME = None
    _CF_LOGICAL_NAME = None

    @classmethod
    def set_cf_info(cls, cf_stack_name=None, cf_logical_name=None):
        if cf_stack_name:
            cls._CF_STACK_NAME = cf_stack_name
        if cf_logical_name:
            cls._LOGICAL_NAME = cf_logical_name

    def _get_cf_stack_name(self):
        return self._cf_stack_name if self._cf_stack_name else self._CF_STACK_NAME

    def _get_cf_logical_name(self):
        return self._cf_logical_name if self._cf_logical_name else self._CF_LOGICAL_NAME

    def __init__(self, _cf_stack_name=None, _cf_logical_name=None, *args, **kwargs):
        self._cf_client = boto3.client('cloudformation')
        self._cf_stack_name = _cf_stack_name
        self._cf_logical_name = _cf_logical_name
        super().__init__(*args, **kwargs)

    def _get_stack_name(self, stack_name=None):
        stack_name = stack_name if stack_name else self._get_cf_stack_name()
        if not stack_name:
            raise RuntimeError("Stack name not set!")
        return stack_name

    def _get_stack_and_logical_names(self, stack_name=None, logical_name=None):
        stack_name = stack_name if stack_name else self._get_cf_stack_name()
        logical_name = logical_name if logical_name else self._get_cf_logical_name()
        if not stack_name or not logical_name:
            raise RuntimeError("Stack name or logical name (or both) not set!")
        return stack_name, logical_name

    def _describe_stack_resource(self, stack_name=None, logical_name=None):
        stack_name, logical_name = self._get_stack_and_logical_names(stack_name=stack_name, logical_name=logical_name)
        response = self._cf_client.describe_stack_resource(StackName=stack_name, LogicalResourceId=logical_name)
        if not response or "StackResourceDetail" not in response:
            raise RuntimeError("Resource does not exist!")
        return response["StackResourceDetail"]

    def _get_physical_resource_id(self, stack_name=None, logical_name=None):
        return self._describe_stack_resource(stack_name=stack_name, logical_name=logical_name)["PhysicalResourceId"]

    def _get_template(self, stack_name=None):
        stack_name = self._get_stack_name(stack_name=stack_name)
        try:
            template = self._cf_client.get_template(StackName=stack_name)["TemplateBody"]
        except ValidationError:
            raise RuntimeError("Unable to retrieve template for stack {}, likely due to it not existing.".format(stack_name))
        return template

    def SCHEMA(self):
        stack_name, logical_name = self._get_stack_and_logical_names()
        template = self._get_template(stack_name=stack_name)
        resources = template["Resources"]
        if logical_name not in resources:
            raise RuntimeError("Stack doesn't contain a table with the given logical name!")
        resource = resources[logical_name]
        table_type = "AWS::DynamoDB::Table"
        if not table_type == resource["Type"]:
            raise RuntimeError("Logical resource {} in stack {} is of type '{}', not type '{}'".format(logical_name, stack_name, resource["Type"], table_type))
        properties = resource["Properties"]
        return properties

    def TABLE_NAME(self):
        return self._get_physical_resource_id()

    def _get_or_create_table_inner(self):
        table_name = self.TABLE_NAME()
        try:
            description = self._client.describe_table(TableName=table_name)
        except ClientError as e:
            raise RuntimeError("Table {} does not exist!".format(table_name))
        return boto3.resource('dynamodb').Table(table_name)

    @classmethod
    def _get_relation_map(cls, obj):
        stack_name, logical_name = obj._get_stack_and_logical_names()
        return {'class':cls.get_classname(), '_cf_stack_name':stack_name, '_cf_logical_name':logical_name}

    @classmethod
    def lazysubclass(cls, stack_name=None, logical_name=None):
        '''
        Returns a class that inherits from this one, with the given default stack and logical names.
        If you just want to have a new object type with no fancy features added, this makes it a one-liner.

        :rtype: Class that inherits from cls
        '''
        class LazyObject(cls):
            _CF_STACK_NAME = stack_name if stack_name else cls._CF_STACK_NAME
            _CF_LOGICAL_NAME = logical_name if logical_name else cls._CF_LOGICAL_NAME
            _CLASSNAME = cls.get_classname()
        return LazyObject
