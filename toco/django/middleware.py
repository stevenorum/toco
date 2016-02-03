#!/usr/bin/env python3

from django.conf import settings
from toco.user import User, SessionToken

class AuthMW(object):
    def __init__(self):
#         print("Settings available in MiddleWare.__init__:")
#         print(settings)
#         print(dir(settings))
#         self.sts_seconds = settings.SECURE_HSTS_SECONDS
#         self.sts_include_subdomains = settings.SECURE_HSTS_INCLUDE_SUBDOMAINS
#         self.content_type_nosniff = settings.SECURE_CONTENT_TYPE_NOSNIFF
#         self.xss_filter = settings.SECURE_BROWSER_XSS_FILTER
#         self.redirect = settings.SECURE_SSL_REDIRECT
#         self.redirect_host = settings.SECURE_SSL_HOST
#         self.redirect_exempt = [re.compile(r) for r in settings.SECURE_REDIRECT_EXEMPT]
        pass

    def process_request(self, request):
        request.user, request.session = SessionToken.get_user_and_session(request.COOKIES.get(SessionToken.CKEY))

    def process_response(self, request, response):
#         print("Request available in MiddleWare.process_response:")
#         print(request)
#         print(request.COOKIES)
#         print('')
#         print(dir(request))
#         print('')
#         print("Response available in MiddleWare.process_response:")
#         print(response)
#         if request.delete_toco_cookie:
#             response.delete_cookie(SESSION_COOKIE_KEY)
#         print(response.cookies)
#         print(response._headers)
#         print('')
#         print(dir(response))
#         print('')
        return response
