#!/usr/bin/env python3

from botocore.exceptions import *
import boto3
from boto3.dynamodb.types import Binary
from boto3.dynamodb.conditions import Key, Attr
import hashlib
import hmac
import os
import time
from toco.object import Object
import uuid


def bytify_binary(b):
    if isinstance(b, Binary):
        return b.__str__()
    return b

def get_hmac_key_01():
    # breaking this out to make it easy to change this to something more secure in the future
    return 'The sun is a miasma of iridescent plasma.'.encode('utf-8')

def hash_password_01(password, salt):
    hash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), bytify_binary(salt)+get_hmac_key_01(), 1000000)
    return hash
    
HASHES = {'01':hash_password_01}

CURRENT_PW_HASH = '01'

class User(Object):

    @staticmethod
    def load_with_auth(email, password):
        u = User(email=email)
        hash = HASHES[u.algo](password, u.salt)
        if hmac.compare_digest(hash, bytify_binary(u.hash)):
            return u
        else:
            return None

    def get_schema(self):
        schema = {
            'TableName':'toco_users',
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

    def get_new_session_token(self, timeout=60*60*24):
        token = SessionToken(id=uuid.uuid1().hex)
        token.user = self.email
        token.expiry = expiry=int(time.time()) + timeout
        token.create()
        return token

    def purge_tokens(self):
        for token in self.active_session_tokens():
            token.expire()

    def set_password(self, password):
        self.salt = os.urandom(64)
        self.algo = CURRENT_PW_HASH
        self.hash = HASHES[self.algo](password, self.salt)

    def active_session_tokens(self):
        now = time.time()
        table = boto3.resource('dynamodb').Table(SessionToken.TABLE_NAME)
        response = table.query(
            IndexName='user',
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=Key('user').eq(self.email) & Key('expiry').gt(int(now))
            )
        return [SessionToken(id=i['id']) for i in response['Items']]


class SessionToken(Object):

    TABLE_NAME = 'toco_session_tokens'

    @staticmethod
    def validate(uuid):
        token = SessionToken(id=uuid)
        if getattr(token, 'user', None):
            return User(email=token.user)
        else:
            return None

    def expire(self):
        self.expiry = int(time.time()-1)
        self.save(force=True)

    def get_schema(self):
        schema = {
            'TableName':SessionToken.TABLE_NAME,
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
