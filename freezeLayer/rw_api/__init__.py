'''
Simple Python SDK for data management on RW API

Examples:

# Initialize
import rw
rw.init(<api_key>, production=True)

# Rename layer
layer = rw.Layer(<layerid>).get()       # fetch layer from API
layer.name = "New name"                 # rename
layer.push()                            # push changes to API

# Copy layer
new_layer = layer.copy(name="New name") # copy and rename
new_layer.push()                        # push new layer to API
'''
from __future__ import unicode_literals
try: from builtins import str
except: from __builtin__ import str

import json
import requests
import os
from .Objects import Dataset, Layer #, Metadata, Widget
from .util import auth, req

# constants
API_URL = os.environ.get('RW_API_URL') or \
    "https://staging-api.globalforestwatch.org/v1/"
PRODUCTION_URL = "https://api.resourcewatch.org/v1/"

def init(token=None, production=False, check_auth=True):
    '''Set api url and token'''
    token = token or os.environ.get('RW_API_KEY') or os.environ.get('rw_api_token')
    url = PRODUCTION_URL if production else API_URL
    return auth(token, url, check_auth)

def getDataset(Id, includes=[]):
    '''Fetch dataset definition from RW API'''
    return Dataset(Id).get(includes)

def getLayer(Id):
    '''Fetch layer definition from RW API'''
    return Layer(Id).get()

def getLayers(app='', published=True, limit=10000, **args):
    '''Get list of layers'''
    app = ','.join(app) if type(app) is list else app
    if app: args['app'] = app
    if published: args['published'] = published
    if limit: args['page[size]'] = limit
    data = req('GET', Layer._GET_ENDPOINT, args)
    return [Layer(r['id'], attributes=r['attributes']) for r in data]

def getDatasets(app='', published=True, includes='', limit=10000, **args):
    '''Get list of datasets'''
    app = ','.join(app) if type(app) is list else app
    includes = ','.join(includes) if type(includes) is list else includes
    if app: args['app'] = app
    if published: args['published'] = published
    if includes: args['includes'] = includes
    if limit: args['page[size]'] = limit
    data = req('GET', Dataset._ENDPOINT, args)
    return [Dataset(r['id'], attributes=r['attributes']) for r in data]


init(check_auth=False)

'''
TODO

def getMetadata(Id):
    pass

def getWidget(Id):
    pass

def getWidgets(app=None, max=10000):
    pass
'''
