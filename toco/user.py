#!/usr/bin/env python3

import binascii
from botocore.exceptions import *
import boto3
from boto3.dynamodb.types import Binary
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import hashlib
import hmac
import logging
import os
import time
from toco.object import Object, RELATION_SUFFIX
import uuid

logger = logging.getLogger(__name__)

def bytify_binary(b):
    '''
    If b is a DynamoDB Binary object instead of a bytestring, this converts it to a bytestring.  Otherwise, it has no effect.
    '''
    if isinstance(b, Binary):
        return b.__str__()
    return b

# The following are broken out so that if we choose to change how passwords are stored in the future,
# it doesn't break existing users.  The next time they update their passwords, they'll get moved onto the newer algorithm.

def get_hmac_key_01():
    '''
    Returns the HMAC key used for V01 passwords.
    '''
    # breaking this out to make it easy to change this to something more secure in the future
    return 'The sun is a miasma of iridescent plasma.'.encode('utf-8')

def hash_password_01(password, salt):
    '''
    Returns the hash of a password given the provided salt, using the V01 hashing technique.
    '''
    hash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), bytify_binary(salt)+get_hmac_key_01(), 1000000)
    return hash
    
HASHES = {'01':hash_password_01}

CURRENT_PW_HASH = '01'

class User(Object):

    @staticmethod
    def load_with_auth(email, password):
        '''
        Verifies a user's password is correct and returns the user object if it is.
        '''
        u = User(email=email)
        if not u.in_db:
            return None
        hash = HASHES[u.algo](password, u.salt)
        if hmac.compare_digest(hash, bytify_binary(u.hash)):
            return u
        else:
            return None

    @classmethod
    def SCHEMA(cls):
        schema = {
            'TableName': 'toco_users',
            'KeySchema': [
                {'AttributeName':'email', 'KeyType':'HASH'},
                ],
            'AttributeDefinitions':[
                {'AttributeName':'email', 'AttributeType':'S'},
                ],
            'ProvisionedThroughput':{
                'ReadCapacityUnits':10,
                'WriteCapacityUnits':10
                },
            }
        return schema

    def get_new_session_token(self, expiry_minutes=60*24, **kwargs):
        token = SessionToken(user=self, expiry_minutes=expiry_minutes, **kwargs)
        token.create()
        return token

    def purge_sessions(self):
        for token in self.active_session_tokens():
            token.expire()

    def set_password(self, password):
        self.salt = os.urandom(64)
        self.algo = CURRENT_PW_HASH
        self.hash = HASHES[self.algo](password, self.salt)

    def active_session_tokens(self):
        now = time.time()
        table = boto3.resource('dynamodb').Table(SessionToken.TABLE_NAME())
        response = table.query(
            IndexName='user',
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=Key('user').eq(self._foreign_key) & Key('expiry').gt(int(now))
            )
        return [SessionToken(id=i['id']) for i in response['Items']]

class SessionToken(Object):

    CKEY = 'TOCO_SESSION'

    def __init__(self, id=None, expiry_minutes=60*24, load_depth=1, **kwargs):
        self.auto_extend = False
        self.extend_minute = 5
        if not id:
            id = binascii.b2a_hex(hashlib.pbkdf2_hmac('sha256', uuid.uuid1().bytes, os.urandom(64), 50)).decode("utf-8")
        super().__init__(id=id, load_depth=load_depth, **kwargs)
        now = int(time.time())
        if not self.__dict__.get('created') and not self.__dict__.get('expiry'):
            # Only add these if it's a new token.
            self.created = now
            self.expiry = now + 60 * expiry_minutes

    def keepalive_if_requested(self):
        """
        If auto-extension was requested when the session was created, this makes sure that the expiration time of the token is at least extend_minutes (constructor parameter, default is 5) minutes in the future.  This is called every time the middleware loads the session.
        """
        if self.auto_extend:
            self.expiry = max(self.expiry, int(time.time()) + 60 * extend_minutes)
            self.save()

    @property
    def expiry_datetime(self):
        return datetime.fromtimestamp(self.expiry)

    @property
    def pretty_created(self):
        return datetime.fromtimestamp(self.created).strftime('%Y/%m/%d %H:%M:%S')

    @property
    def pretty_expiry(self):
        return datetime.fromtimestamp(self.expiry).strftime('%Y/%m/%d %H:%M:%S')

    @staticmethod
    def validate(uuid):
        return SessionToken.get_user_and_session[0]

    @staticmethod
    def get_user_and_session(uuid):
        token = SessionToken(id=uuid)
        if token.expiry < time.time():
            return None, None
        else:
            return token.user, token

    def expire(self):
        self.expiry = int(time.time()-1)
        self.save(force=True)

    @classmethod
    def SCHEMA(cls):
        schema = {
            'TableName': 'toco_session_tokens',
            'KeySchema': [
                {'AttributeName':'id', 'KeyType':'HASH'},
                ],
            'AttributeDefinitions':[
                {'AttributeName':'id', 'AttributeType':'S'},
                {'AttributeName':'user', 'AttributeType':'S'},
                {'AttributeName':'expiry', 'AttributeType':'N'},
                ],
            'GlobalSecondaryIndexes':[
                {
                    'IndexName': 'user',
                    'KeySchema': [
                        {
                            'AttributeName': 'user',
                            'KeyType': 'HASH'
                            },
                        {
                            'AttributeName': 'expiry',
                            'KeyType': 'RANGE'
                            },
                        ],
                    'Projection': {
                        'ProjectionType': 'ALL',
                        },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                        }
                    },
                ],
            'ProvisionedThroughput':{
                'ReadCapacityUnits':10,
                'WriteCapacityUnits':10
                },
            }
        return schema

class PasswordResetRequest(Object):

    def __init__(self, id=None, user=None, expiry_minutes=60*24, **kwargs):
        if not id:
            id = binascii.b2a_hex(hashlib.pbkdf2_hmac('sha256', uuid.uuid1().bytes, os.urandom(64), 50)).decode("utf-8")
        super().__init__(id=id,**kwargs)
        if user and not self.__dict__.get('user'):
            self.user = user
        now = int(time.time())
        if not self.__dict__.get('created') and not self.__dict__.get('expiry'):
            # Only add these if it's a new request.
            self.created = now
            self.expiry = now + 60 * expiry_minutes

    def expire(self):
        self.expiry = int(time.time()-1)
        self.save(force=True)

    @classmethod
    def SCHEMA(cls):
        schema = {
            'TableName': 'toco_password_reset_requests',
            'KeySchema': [
                {'AttributeName':'id', 'KeyType':'HASH'},
                ],
            'AttributeDefinitions':[
                {'AttributeName':'id', 'AttributeType':'S'},
                {'AttributeName':'user', 'AttributeType':'S'},
                {'AttributeName':'expiry', 'AttributeType':'N'},
                ],
            'GlobalSecondaryIndexes':[
                {
                    'IndexName': 'user',
                    'KeySchema': [
                        {
                            'AttributeName': 'user',
                            'KeyType': 'HASH'
                            },
                        {
                            'AttributeName': 'expiry',
                            'KeyType': 'RANGE'
                            },
                        ],
                    'Projection': {
                        'ProjectionType': 'ALL',
                        },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                        }
                    },
                ],
            'ProvisionedThroughput':{
                'ReadCapacityUnits':10,
                'WriteCapacityUnits':10
                },
            }
        return schema
