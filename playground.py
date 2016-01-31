#!/usr/bin/env python3

from toco.user import User, SessionToken

email='test@mail.com'
password='guest'

# u = User(email=email)
# u.set_password('guest')
# u.save()

def dump_token(t):
    print(t.id)
    print(t.user)
    print(t.expiry)

u1 = User.load_with_auth(email, password)
u2 = User.load_with_auth(email, password)
u1.setting = "foobar"
u1.save()
print(u1.__dict__)
print(u2.__dict__)
u2.reload()
print(u2.__dict__)



# u.purge_tokens()
# token1 = u.get_new_session_token()
# dump_token(token1)
# token2 = u.get_new_session_token(expiry_minutes = 5)
# dump_token(token2)



# u.purge_tokens()
# # #u.create()
# # print(token)
# print(token.id)
# # print(token.user)
# # print(token.expiry)

# uuid1='8e0762fac60d11e5bee7ac87a309da64'
# uuid2=token.id
# #uuid='8e0762fac60d11e5bee7ac87a309da63'
# print(SessionToken.validate(uuid1))
# print(SessionToken.validate(uuid2))
