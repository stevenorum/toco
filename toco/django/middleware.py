#!/usr/bin/env python3

import logging
from toco.user import User, SessionToken

logger = logging.getLogger(__name__)

class AuthMW(object):
    def __init__(self):
        pass

    def process_request(self, request):
        request.user, request.session = SessionToken.get_user_and_session(uuid=request.COOKIES.get(SessionToken.CKEY))
        request.session.keepalive_if_requested()
 
    def process_response(self, request, response):
        if request.COOKIES.get(SessionToken.CKEY) and not request.user:
            response.delete_cookie(SessionToken.CKEY)
        return response
