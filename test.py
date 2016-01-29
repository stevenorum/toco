#!/usr/bin/env python3

from toco.user import User, SessionToken

email='test@mail.com'

# u = User(email=email)
# u.set_password('guest')
# u.save()

print(User.load_with_auth(email, 'guest'))
print(User.load_with_auth(email, 'guest1'))


# token = u.get_new_session_token()

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
