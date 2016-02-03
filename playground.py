#!/usr/bin/env python3

# import inspect
from toco.user import User, SessionToken

email='test@mail.com'
password='guest'

u = User.load_with_auth(email, password)
        
u.purge_sessions()
token = u.get_new_session_token()
