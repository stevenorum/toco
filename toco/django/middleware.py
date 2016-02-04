#!/usr/bin/env python3

import logging
from toco.user import User, SessionToken

logger = logging.getLogger(__name__)

class AuthMW(object):
    def __init__(self):
        pass

    def process_request(self, request):
        print("process_request beginning.")
        request.session = getattr(request, 'session', None)
        request.user = getattr(request, 'user', None)
        print(request.session)
        print(request.user)
        if request.COOKIES.get(SessionToken.CKEY):
#         if request.COOKIES.get(SessionToken.CKEY) and not (request.session and request.user):
            request.session = SessionToken(id=request.COOKIES.get(SessionToken.CKEY), recurse=1)
            print(request.session)
            print(dir(request.session))
            request.user = request.session.user
            request.session.keepalive_if_requested()
        print(request.session)
        print(request.user)
        print("process_request ending.")
 
    def process_response(self, request, response):
        print("process_response beginning.")
        if request.COOKIES.get(SessionToken.CKEY) and not request.user:
            response.delete_cookie(SessionToken.CKEY)
        print("process_response ending.")
        return response
