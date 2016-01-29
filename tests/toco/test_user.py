#!/usr/bin/env python3

import toco
from toco import *

from mock import call, MagicMock, Mock, patch
from nose.tools import assert_equals, raises
from unittest import TestCase

class UserTests(TestCase):

    client_mocks = {
        'dynamodb':MagicMock()
        }

    def boto3_client(self, arg):
        return client_mocks.get(arg, None)

    def setUp(self):
        self.mock_boto3_client = MagicMock(side_effect=boto3_client)
        patch('toco.boto3.client', self.mock_boto3_client).start()
        self.region = 'us-east-1'

    def tearDown(self):
        patch.stopall()

    def test_GIVEN_user_does_not_exists_WHEN_user_created_THEN_user_created(self):
        # GIVEN
        # WHEN
        # THEN
        assert_equals(True,False)

    def test_GIVEN_user_does_not_exists_WHEN_user_saved_THEN_user_created(self):
        # GIVEN
        # WHEN
        # THEN
        assert_equals(True,False)

    def test_GIVEN_user_does_not_exists_WHEN_user_updated_THEN_error_raised(self):
        # GIVEN
        # WHEN
        # THEN
        assert_equals(True,False)

    def test_GIVEN_user_already_exists_WHEN_user_created_THEN_error_raised(self):
        # GIVEN
        # WHEN
        # THEN
        assert_equals(True,False)

    def test_GIVEN_user_already_exists_WHEN_user_updated_THEN_user_updated(self):
        # GIVEN
        # WHEN
        # THEN
        assert_equals(True,False)

    def test_GIVEN_user_already_exists_WHEN_user_saved_THEN_user_saved(self):
        # GIVEN
        # WHEN
        # THEN
        assert_equals(True,False)
