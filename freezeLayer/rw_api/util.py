'''util.py'''
from __future__ import unicode_literals
try: from builtins import str
except: from __builtin__ import str

import requests
import json
import logging

# global vars
_api_url = None
_api_key = None

def auth(token, url, check_auth=True):
    global _api_key, _api_url
    _api_key = token
    _api_url = url
    if check_auth:
        try:
            req('GET', '../auth/check-logged', auth=True, raw=True)
            return True
        except requests.HTTPError as e:
            logging.warning('Failed to authenticate')
            logging.warning(e)
            return False

def req(method, endpoint, payload=None, auth=False, raw=False):
    if _api_url is None:
        raise(Exception('Uninitialized. Initialize with rw_api.init(<key>)'))

    url = urljoin(_api_url, endpoint)
    if method.lower() == 'get' and auth == False:
        response = requests.get(url, payload)
    else:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {token}'.format(token=_api_key)
        }
        if type(payload) is dict: payload = json.dumps(payload)
        response = requests.request(method, url, data=payload, headers=headers)

    response.raise_for_status()
    if raw:
        return response.text
    return response.json()['data']

def urljoin(*args):
    return '/'.join(x.strip('/') for x in args)
