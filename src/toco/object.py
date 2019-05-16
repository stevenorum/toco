#!/usr/bin/env python3

import base64
from botocore.exceptions import *
from boto3.dynamodb.conditions import Key, Attr, Or
from boto3.dynamodb.types import TypeSerializer
import boto3
import copy
from datetime import datetime
import decimal
import inspect
import json
import logging
import os
import traceback

VERSION_KEY = 'version_toco_'

JSON_CLASS = '_class_toco'
JSON_FKEY = '_fkey_toco'

RELATION_SUFFIX = '_rel_toco_'
FKEY_PREFIX = 'toco_fkey='

FKEY_EMPTY_STRING = FKEY_PREFIX + "EMPTY-STRING"
CONSTANT_FKEYS = {FKEY_EMPTY_STRING: ""}

DATETIME_FORMAT = "datetime:%Y-%m-%dT%H:%M:%S.%fZ"

logger = logging.getLogger(__name__)

def load_python_class_if_applicable(value):
    if value and isinstance(value, str) and value.startswith("datetime:"):
        try:
            return datetime.strptime(value, DATETIME_FORMAT)
        except:
            pass
    return value

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

def is_foreign_key(fkey):
    '''
    Determines whether a given object is a toco foreign key, or a string containing a classname and the keys necessary to load an object from DynamoDB.

    :param fkey: An object that may or may not be a toco foreign key.
    :rtype: toco object
    '''
    if not isinstance(fkey, str) or not fkey.startswith(FKEY_PREFIX):
        return False
    if fkey in CONSTANT_FKEYS:
        return True
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

def load_from_fkey(fkey, **kwargs):
    '''
    If fkey is a toco foreign key, load it from DynamoDB.

    :param fkey: A toco foreign key.
    :rtype: toco object
    '''
    if not is_foreign_key(fkey):
        return None
    if fkey in CONSTANT_FKEYS:
        return CONSTANT_FKEYS[fkey]
    obj = json.loads(fkey[len(FKEY_PREFIX):])
    key = obj['key']
    clazzname = obj['class']
    del obj['key']
    del obj['class']
    obj.update(key)
    obj.update(**kwargs)
    return get_class(clazzname)._from_fkey(**obj)

def ensure_ddbsafe(d):
    ts = TypeSerializer()
    if isinstance(d, str):
        if len(d) == 0:
            return FKEY_EMPTY_STRING
        else:
            return d
    if isinstance(d, dict):
        return {k:ensure_ddbsafe(d[k]) for k in d}
    elif isinstance(d, list):
        return [ensure_ddbsafe(e) for e in d]
    elif isinstance(d, float):
        return decimal.Decimal(d)
    elif isinstance(d, datetime):
        return d.strftime(DATETIME_FORMAT)
    else:
        return d

def load_constant_fkeys(d):
    if isinstance(d, str) and d in CONSTANT_FKEYS:
        return CONSTANT_FKEYS[d]
    elif isinstance(d, dict):
        return {k:load_constant_fkeys(d[k]) for k in d}
    elif isinstance(d, list):
        return [load_constant_fkeys(e) for e in d]
    else:
        return d

class blob(dict):
    RESERVED_KEYS = ["__predefined_attributes__","__raise_on_miss","_blob__raise_on_miss"]
    def __init__(self, *args, **kwargs):
        __raise_on_miss = bool(kwargs.get("raise_on_miss"))
        if "raise_on_miss" in kwargs:
            del kwargs["raise_on_miss"]
        super().__init__(*args, **kwargs)
        self.__predefined_attributes__ = [a for a in dir(self)]
        self.__predefined_attributes__.append("__predefined_attributes__")
        self.__raise_on_miss = __raise_on_miss
        self.__predefined_attributes__.append("__raise_on_miss")
        for key in self.keys():
            if not key in self.__predefined_attributes__:
                setattr(self, key, self[key])
                pass
            pass
        pass

    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)
        if name not in blob.RESERVED_KEYS:
            self.__setitem__(name, value)

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except:
            if object.__getattribute__(self, "__raise_on_miss"):
                return self.__getitem__(name)
            else:
                return self.get(name)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if not key in self.__predefined_attributes__:
            object.__setattr__(self, key, self[key])

    def __delitem__(self, key):
        super().__delitem__(key)
        if not key in self.__predefined_attributes__:
            delattr(self, key)

class BaseTocoObject(object):
    """
    Holder class for a bunch of class methods and stuff like that.
    """
    _SCHEMA_CACHE = None
    _TABLE_CACHE = None
    _CLASSNAME = None
    _REQUIRED_ATTRS = []
    _COMPOUND_ATTRS = {}

    @classmethod
    def _from_dict(cls, d):
        if d.get(JSON_FKEY, None):
            return load_from_fkey(d[JSON_FKEY])
        if d.get(JSON_CLASS, None):
            return get_class(d[JSON_CLASS])(**d)
        return cls(**d)

    @classmethod
    def _encode_nexttoken(cls, key):
        # TODO: replace the json step with something that'll work for any input
        # Convert to a string
        # This won't work for a lot of valid keys due to the json serialization step
        key = json.dumps(key)
        # base64 encode it to make it safer to handle
        key = base64.urlsafe_b64encode(key.encode("utf-8")).decode("utf-8")
        # strip the padding, as we can easily re-add it later
        key = key.replace("=","")
        return key

    @classmethod
    def _decode_nexttoken(cls, key):
        # TODO: replace the json step with something that'll work for any input
        # re-add the padding
        key = key + "=" * ((-1*len(key))%16)
        # convert if back from base64-encoded bytes to the underlying string
        key = base64.urlsafe_b64decode(key.encode("utf-8")).decode("utf-8")
        # load the string back into a dict
        # This won't work for a lot of valid keys due to the json serialization step
        key = json.loads(key)
        return key

    @classmethod
    def _parse_items(cls, response):
        items = []
        for item in response.get("Items",[]):
            params = dict(item)
            params["_in_db"] = True
            params["_attempt_load"] = False
            items.append(cls(**params))
        return items

    @classmethod
    def _preprocess_search_params(cls, **kwargs):
        params = dict(kwargs)
        if params.get("NextToken", None) and not params.get("ExclusiveStartKey", None):
            params["ExclusiveStartKey"] = cls._decode_nexttoken(params["NextToken"])
        if "NextToken" in params:
            del params["NextToken"]
        if not params.get("KeyConditionExpression", None):
            hashname, rangename = cls._HASH_AND_RANGE_KEYS(index_name = params.get("IndexName", None))
            if params.get(hashname) and not params.get("HashKey", None):
                params["HashKey"] = params[hashname]
                del params[hashname]
            if params.get(rangename) and not params.get("RangeKey", None):
                params["RangeKey"] = params[rangename]
                del params[rangename]
            if params.get("HashKey", None) and not params.get("KeyConditionExpression", None):
                hkc = Key(hashname).eq(params["HashKey"])
                if params.get("RangeKey", None):
                    rk = params["RangeKey"]
                    if isinstance(rk,(list, tuple)):
                        rkc = getattr(Key(rangename),rk[0])(*rk[1:])
                    else:
                        rkc = Key(rangename).eq(rk_arg)
                    kce = hkc & rkc
                else:
                    kce = hkc
                params["KeyConditionExpression"] = kce
        if "HashKey" in params:
            del params["HashKey"]
        if "RangeKey" in params:
            del params["RangeKey"]
        return params

    @classmethod
    def _postprocess_search_results(cls, results):
        response = {
            "Items":cls._parse_items(results),
            "NextToken":None,
            "RawResponse":results
        }
        if results.get("LastEvaluatedKey", None):
            response["NextToken"] = cls._encode_nexttoken(results["LastEvaluatedKey"])
        return response

    @classmethod
    def scan(cls, **kwargs):
        params = cls._preprocess_search_params(**kwargs)
        results = cls.TABLE().scan(**params)
        return cls._postprocess_search_results(results)

    @classmethod
    def query(cls, **kwargs):
        params = cls._preprocess_search_params(**kwargs)
        results = cls.TABLE().query(**params)
        return cls._postprocess_search_results(results)

    @classmethod
    def load(cls, **kwargs):
        obj = cls(_attempt_load=True, **kwargs)
        if obj._in_db:
            return obj
        return None

    @classmethod
    def SCHEMA(cls, use_cache=True):
        if cls._SCHEMA_CACHE and use_cache:
            return cls._SCHEMA_CACHE
        schema = cls._SCHEMA()
        cls._SCHEMA_CACHE = schema
        return schema

    @classmethod
    def _SCHEMA(cls, use_cache=True):
        raise NotImplementedError("Each subclass must implement this on their own.")

    @classmethod
    def TABLE_NAME(cls, use_cache=True):
        schema = cls.SCHEMA(use_cache=use_cache)
        return schema.get('TableName')

    @classmethod
    def CLASS_NAME(cls):
        return cls._CLASSNAME if cls._CLASSNAME else "{module}.{name}".format(module=cls.__module__, name=cls.__name__)

    @classmethod
    def TABLE(cls):
        if not cls._TABLE_CACHE:
            cls._TABLE_CACHE = boto3.resource('dynamodb').Table(cls.TABLE_NAME())
        return cls._TABLE_CACHE

    @classmethod
    def create_table(cls):
        boto3.client("dynamodb").create_table(**cls._SCHEMA())

    @classmethod
    def _get_required_attributes(cls):
        attrs = []
        hashkn, rangekn = cls._HASH_AND_RANGE_KEYS()
        if hashkn:
            attrs.append(hashkn)
        if rangekn:
            attrs.append(rangekn)
        attrs.extend(cls._REQUIRED_ATTRS)
        return attrs

    @classmethod
    def _HASH_AND_RANGE_KEYS(cls, index_name=None):
        schema = cls._SCHEMA()
        key_schema = schema['KeySchema']
        if index_name:
            gsis = schema.get("GlobalSecondaryIndexes", [])
            matches = [gsi for gsi in gsis if gsi["IndexName"] == index_name]
            if len(matches) == 0:
                raise RuntimeError("No index with the name '{index_name}' found!".format(index_name=index_name))
            key_schema = matches[0]["KeySchema"]
        hash = [h['AttributeName'] for h in key_schema if h['KeyType']=='HASH'][0]
        ranges = [r['AttributeName'] for r in key_schema if r['KeyType']=='RANGE']
        range = ranges[0] if ranges else None
        return hash, range

    @classmethod
    def _get_class_relation_map(cls, obj):
        return {'class':cls.CLASS_NAME(), 'key':obj._get_key_dict()}

    @classmethod
    def _from_fkey(cls, **kwargs):
        obj = cls(**kwargs)
        obj._needs_reloaded = True
        return obj

    @classmethod
    def _add_compound_attr(cls, attrname, attrfunc, save=False):
        cls._COMPOUND_ATTRS[attrname] = {"func":attrfunc,"save":save}

    @classmethod
    def _remove_compound_attr(cls, attrname):
        if attrname in cls._COMPOUND_ATTRS:
            del cls._COMPOUND_ATTRS[attrname]

class TocoObject(BaseTocoObject):
    '''
    Base class for all DynamoDB-storable toco objects.  Cannot itself be instantiated.

    The only thing that a subclass is required to implement is the classmethod SCHEMA, and it must return a dict that can be passed to client.create_table(**schema) and succeed.

    Constructor args:

    :param kwargs: Keys for an object, and any attributes to attach to that object.
    :rtype: toco object
    '''
    def __init__(self, _in_db=False, _attempt_load=True, **kwargs):
        self._needs_reloaded = False
        self._serialize_as_dict = True
        self._raise_on_getattr_miss = False
        self._obj_dict = blob()
        self._fkey_cache = {}
        self._obj_loaded = {}
        self._obj_updates = {}

        setattr(self, VERSION_KEY, 0)
        self._in_db = _in_db

        if _attempt_load:
            try:
                description = self.__class__.TABLE().get_item(Key=self._get_key_dict(kwargs))
            except ClientError as e:
                description = {}
            if description.get('Item'):
                self._update_attrs(**description['Item'])
                self._clear_update_record()
                self._in_db = True
                # Don't treat init-time changes as real changes if they match the DB.
        self._update_attrs_changed(**kwargs)

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            if isinstance(value, TocoObject):
                self._obj_dict[name] = value._foreign_key()
                self._fkey_cache[name] = value
            else:
                self._obj_dict[name] = value
                if name in self._fkey_cache:
                    del self._fkey_cache[name]
            if name != VERSION_KEY:
                # The version key is special and shouldn't be tracked
                self._obj_updates[name] = value

    def __getattribute__(self, name):
        if name.startswith("_"):
            # or name in self.__dict__(name not in self._obj_dict and name not in self.__class__._COMPOUND_ATTRS):
            if name in object.__getattribute__(self, "__dict__"):
                return object.__getattribute__(self, "__dict__").get(name)
            return object.__getattribute__(self, name)
        else:
            if self._needs_reloaded:
                self._reload()
                self._needs_reloaded = False
            if name in self._obj_dict:
                value = self._obj_dict[name]
                if is_foreign_key(value):
                    obj = load_from_fkey(value)
                    self._fkey_cache[name] = obj
                    return obj
                else:
                    return load_python_class_if_applicable(self._obj_dict[name])
            elif name in self.__class__._COMPOUND_ATTRS:
                return self.__class__._COMPOUND_ATTRS[name]["func"](self)
            else:
                try:
                    if name in object.__getattribute__(self, "__dict__"):
                        return object.__getattribute__(self, "__dict__").get(name)
                    return object.__getattribute__(self, name)
                except AttributeError as e:
                    if self._raise_on_getattr_miss:
                        raise e
                    else:
                        return None

    def __delattr__(self, name):
        if name in self._obj_dict:
            del self._obj_dict[name]
            if name in self._fkey_cache:
                del self._fkey_cache[name]
            self._obj_updates[name] = None
        else:
            object.__delattr__(self, name)

    def _update_attrs(self, **kwargs):
        kwargs = load_constant_fkeys(kwargs)
        for k in kwargs:
            setattr(self, k, kwargs[k])

    def _update_attrs_changed(self, **kwargs):
        for k in kwargs:
            if getattr(self, k) != kwargs[k]:
                setattr(self, k, kwargs[k])

    def _clear_update_record(self):
        self._obj_loaded = self._obj_dict
        self._obj_updates = {}

    def _get_dict_to_save(self):
        dict_to_save = copy.copy(self._obj_dict)
        compattrs = self.__class__._COMPOUND_ATTRS
        for attrname in compattrs:
            if attrname in dict_to_save:
                continue
            elif compattrs[attrname].get("save", False):
                dict_to_save[attrname] = compattrs[attrname]["func"](self)
        return dict_to_save

    def _get_data_dict(self):
        d = self._get_dict_to_save()
        keys = list(d.keys())
        for k in keys:
            if k.startswith("toco_") or k.endswith("_toco") or "_toco_" in k or k.startswith("_"):
                del d[k]
        return d

    def _my_hash_and_range(self):
        hash_keyname, range_keyname = self.__class__._HASH_AND_RANGE_KEYS()
        hash_key = getattr(self, hash_keyname)
        range_key = getattr(self, range_keyname) if range_keyname else None
        return hash_key, range_key

    def _get_key_dict(self, dictionary=None):
        hash_keyname, range_keyname = self.__class__._HASH_AND_RANGE_KEYS()
        keys = {}
        dictionary = dictionary if dictionary else self._obj_dict
        for k in (hash_keyname, range_keyname):
            if k and k in dictionary.keys():
                # I'm explicitly bypassing the getter here in the off chance either hash or range is a foreign key
                keys[k] = dictionary[k]
        return keys

    def _get_relation_map(self):
        classes = []
        new_classes = [self.__class__]
        has_method = True
        while new_classes:
            bases = []
            for clazz in new_classes:
                if clazz not in classes and hasattr(clazz, "_get_class_relation_map"):
                    classes.append(clazz)
                    bases.extend(clazz.__bases__)
            new_classes = bases
        relation_map = {}
        for clazz in classes[::-1]:
            relation_map.update(clazz._get_class_relation_map(self))
        return relation_map

    def _foreign_key(self):
        '''
        The foreign key necessary to load this object from DynamoDB.
        '''
        return FKEY_PREFIX+json.dumps(self._get_relation_map(), sort_keys=True, separators=(',', ':'))

    @classmethod
    def _json_deserialize(cls, fkey):
        return load_from_fkey(fkey, _attempt_load=False)

    def _json_serialize(self):
        if self._serialize_as_dict:
            d = self._get_dict_to_save()
            d[JSON_CLASS] = self.__class__.CLASS_NAME()
            d[JSON_FKEY] = self._foreign_key()
            return d, "dict"
        else:
            return self._foreign_key(), self.__class__.CLASS_NAME()

    def _save(self, force=False, save_if_missing=True, save_if_existing=True, only_if_updated=False):
        if not save_if_missing and not save_if_existing:
            raise RuntimeError("At least one of save_if_missing and save_if_existing must be true.")

        if only_if_updated and not self._obj_updates:
            return self

        old_version = getattr(self, VERSION_KEY)
        create_condition = Attr(VERSION_KEY).not_exists()
        if force:
            update_condition = Attr(VERSION_KEY).exists()
        else:
            update_condition = Attr(VERSION_KEY).eq(old_version)
        CE = None
        if force and save_if_missing and save_if_existing:
            pass
        elif save_if_missing and save_if_existing:
            CE = Or(create_condition, update_condition)
        elif save_if_existing:
            CE = update_condition
        else:
            # If we're here, we know that create_condition=True
            CE = create_condition
        try:
            setattr(self, VERSION_KEY, old_version+1)
            if CE:
                self._store(CE)
            else:
                self._store()
            self._clear_update_record()
            self._in_db = True
            return self
        except ClientError as e:
            setattr(self, VERSION_KEY, old_version)
            raise e

    def _update(self, force=False):
        return self._save(force=force, save_if_existing=True, save_if_missing=False)

    def _create(self):
        return self._save(force=force, save_if_existing=False, save_if_missing=True)

    def _store(self, CE=None):
        dict_to_save = self._get_dict_to_save()
        required = self._get_required_attributes()
        missing = [r for r in required if not r in dict_to_save or not dict_to_save[r]]
        if missing:
            raise RuntimeError('The following attributes are missing and must be added before saving: '+', '.join(missing))
        dict_to_save = ensure_ddbsafe(dict_to_save)
        if CE:
            self.__class__.TABLE().put_item(Item=dict_to_save, ConditionExpression=CE)
        else:
            self.__class__.TABLE().put_item(Item=dict_to_save)
        return self

    def _delete(self, CE=None):
        if CE:
            return self.__class__.TABLE().delete_item(Key=self._get_key_dict(), ConditionExpression=CE)
        else:
            return self.__class__.TABLE().delete_item(Key=self._get_key_dict())

    def _load(self):
        b = blob()
        b.update(self.__class__.TABLE().get_item(Key=self._get_key_dict()).get("Item", {}))
        # return self.__class__.TABLE().get_item(Key=self._get_key_dict()).get("Item", {})
        return b

    def _reload(self):
        '''
        Reloads the item's attributes from DynamoDB, replacing whatever's currently in the object.
        '''
        self._obj_dict = self._load()
        self._in_db = True
        self._clear_update_record()
        return self

class CFObject(TocoObject):
    '''
    Base class for toco objects that are based on tables created in a CloudFormation stack.
    '''

    _CF_STACK_NAME = None
    _CF_LOGICAL_NAME = None
    _CF_CLIENT = boto3.client('cloudformation')
    _CF_TEMPLATE = None
    _CF_RESOURCES = {}

    @classmethod
    def _set_cf_info(cls, cf_stack_name=None, cf_logical_name=None):
        if cf_stack_name:
            cls._CF_STACK_NAME = cf_stack_name
        if cf_logical_name:
            cls._LOGICAL_NAME = cf_logical_name

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def _get_stack_name(cls, stack_name=None):
        stack_name = stack_name if stack_name else cls._CF_STACK_NAME
        if not stack_name:
            raise RuntimeError("Stack name not set!")
        return stack_name

    @classmethod
    def _get_stack_and_logical_names(cls, stack_name=None, logical_name=None):
        stack_name = stack_name if stack_name else cls._CF_STACK_NAME
        logical_name = logical_name if logical_name else cls._CF_LOGICAL_NAME
        if not stack_name or not logical_name:
            raise RuntimeError("Stack name or logical name (or both) not set!")
        return stack_name, logical_name

    @classmethod
    def _describe_stack_resource(cls, stack_name=None, logical_name=None):
        stack_name, logical_name = cls._get_stack_and_logical_names(stack_name=stack_name, logical_name=logical_name)
        if logical_name not in cls._CF_RESOURCES:
            logging.warn("Cache miss loading _CF_RESOURCE for class {}".format(cls))
            response = cls._CF_CLIENT.describe_stack_resource(StackName=stack_name, LogicalResourceId=logical_name)
            if not response or "StackResourceDetail" not in response:
                raise RuntimeError("Resource does not exist!")
            cls._CF_RESOURCES[logical_name] = response["StackResourceDetail"]
        else:
            logging.info("Cache hit loading _CF_RESOURCE for class {}".format(cls))
        return cls._CF_RESOURCES[logical_name]

    @classmethod
    def _get_physical_resource_id(cls, stack_name=None, logical_name=None):
        return cls._describe_stack_resource(stack_name=stack_name, logical_name=logical_name)["PhysicalResourceId"]

    @classmethod
    def _get_template(cls, stack_name=None):
        stack_name = cls._get_stack_name(stack_name=stack_name)
        if not getattr(cls, "_CF_TEMPLATE"):
            logging.warn("Cache miss loading _CF_TEMPLATE for class {}".format(cls))
            try:
                template = cls._CF_CLIENT.get_template(StackName=stack_name)["TemplateBody"]
                setattr(cls, "_CF_TEMPLATE", template)
            except ValidationError:
                raise RuntimeError("Unable to retrieve template for stack {}, likely due to it not existing.".format(stack_name))
        else:
            logging.info("Cache hit loading _CF_TEMPLATE for class {}".format(cls))
        return cls._CF_TEMPLATE

    @classmethod
    def _clear_cf_cache(cls):
        setattr(cls, "_CF_TEMPLATE", None)
        setattr(cls, "_CF_RESOURCES", {})

    @classmethod
    def _SCHEMA(cls):
        stack_name, logical_name = cls._get_stack_and_logical_names()
        template = cls._get_template(stack_name=stack_name)
        resources = template["Resources"]
        if logical_name not in resources:
            raise RuntimeError("Stack doesn't contain a table with the given logical name!")
        resource = resources[logical_name]
        table_type = "AWS::DynamoDB::Table"
        if not table_type == resource["Type"]:
            raise RuntimeError("Logical resource {} in stack {} is of type '{}', not type '{}'".format(logical_name, stack_name, resource["Type"], table_type))
        properties = dict(resource["Properties"])
        properties["TableName"] = cls._get_physical_resource_id()
        return properties

    @classmethod
    def _get_class_relation_map(cls, obj):
        stack_name, logical_name = obj._get_stack_and_logical_names()
        return {'class':cls.CLASS_NAME(), '_cf_stack_name':stack_name, '_cf_logical_name':logical_name}

    @classmethod
    def _from_fkey(cls, _cf_stack_name, _cf_logical_name, **kwargs):
        return cls.lazysubclass(stack_name=_cf_stack_name, logical_name=_cf_logical_name)(**kwargs)

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
            _CLASSNAME = cls.CLASS_NAME()
        return LazyObject
