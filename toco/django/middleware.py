#!/usr/bin/env python3

from django.conf import settings

class AuthMW(object):
    def __init__(self):
        print("Settings available in MiddleWare.__init__:")
        print(settings)
        print(dir(settings))
        self.sts_seconds = settings.SECURE_HSTS_SECONDS
        self.sts_include_subdomains = settings.SECURE_HSTS_INCLUDE_SUBDOMAINS
        self.content_type_nosniff = settings.SECURE_CONTENT_TYPE_NOSNIFF
        self.xss_filter = settings.SECURE_BROWSER_XSS_FILTER
        self.redirect = settings.SECURE_SSL_REDIRECT
        self.redirect_host = settings.SECURE_SSL_HOST
        self.redirect_exempt = [re.compile(r) for r in settings.SECURE_REDIRECT_EXEMPT]

    def process_request(self, request):
        print("Request available in MiddleWare.process_request:")
        print(request)
        print(request.COOKIES)
#         print(dir(request))

    def process_response(self, request, response):
        print("Request available in MiddleWare.process_response:")
        print(request)
        print(request.COOKIES)
#         print(dir(request))
        print("Response available in MiddleWare.process_response:")
        print(response)
        response.cookies['toco_session'] = 'foobar'
        print(response.cookies)
        print(response._headers)
#         print(dir(response))
        return response
