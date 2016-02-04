#!/usr/bin/env python3

import logging
from toco.user import User, SessionToken

logger = logging.getLogger(__name__)

class AuthMW(object):
    def __init__(self):
        pass

    def process_request(self, request):
        request.session = getattr(request, 'session', None)
        request.user = getattr(request, 'user', None)
        if request.COOKIES.get(SessionToken.CKEY) and not (request.session and request.user and request.session.id == request.COOKIES.get(SessionToken.CKEY)):
            request.session = SessionToken(id=request.COOKIES.get(SessionToken.CKEY), recurse=1)
            request.user = request.session.user
            request.session.keepalive_if_requested()
 
    def process_response(self, request, response):
        if request.COOKIES.get(SessionToken.CKEY) and not request.user:
            response.delete_cookie(SessionToken.CKEY)
        return response
