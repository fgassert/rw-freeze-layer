'''
Object handlers for Resource Watch API
'''
from __future__ import unicode_literals
try: from builtins import str
except: from __builtin__ import str

import json
import difflib
from .util import req, urljoin

class rwObj(object):
    _GET_ENDPOINT = None
    _ENDPOINT = None

    def __init__(self, Id=None, name=None, apps=[], attributes=None):
        attributes = attributes or {}
        self._data = {'id':Id, 'attributes':attributes}
        if name: self.name = name
        if apps: self.apps = apps

    @property
    def attributes(self): return self._data['attributes']
    @attributes.setter
    def attributes(self, attributes): self._data['attributes'] = attributes
    @property
    def apps(self): return self.attributes['application']
    @apps.setter
    def apps(self, apps): self.attributes['application'] = apps
    @property
    def name(self): return self.attributes['name']
    @name.setter
    def name(self, name): self.attributes['name'] = name
    @property
    def published(self): return self.attributes['published']
    @published.setter
    def published(self, published): self.attributes['published'] = published
    @property
    def protected(self): return self.attributes['protected']
    @protected.setter
    def protected(self, protected): self.attributes['protected'] = protected

    # Read only properties
    @property
    def Id(self): return self._data['id']
    @property
    def provider(self): return self.attributes['provider']
    @property
    def slug(self): return self.attributes['slug']

    # Private methods
    def _getEndpoint(self):
        '''Return endpoint for GET requests'''
        return urljoin(self._GET_ENDPOINT or self._ENDPOINT, self.Id)

    def _postEndpoint(self):
        '''Return endpoint for all other requsts'''
        if self.Id:
            return urljoin(self._ENDPOINT, Id)
        return self._ENDPOINT

    def _validatePost(self):
        '''Check if required fields are defined. Called before posting'''
        pass

    def _validatePatch(self):
        '''Check if required fields are defined. Called before patching'''
        pass

    def __repr__(self):
        return '<{}: {}>'.format(type(self), self.name)

    # Public Methods
    def json(self, **args):
        '''Get json representation of object'''
        return json.dumps(self._data, **args)

    def attrJson(self, **args):
        '''Get json representation of object attributes'''
        return json.dumps(self.attributes, **args)

    def fromJson(self, data):
        ''''''
        if isinstance(data, str): data = json.loads(data)
        self.__init__(data['id'], attributes=data['attributes'])

    def copy(self, name=None, **args):
        '''Returns a copy of this object with new name and no Id or slug'''
        # deep copy attribute dict
        attrs = json.loads(self.attrJson())
        if 'slug' in attrs: del attrs['slug']
        return type(self)(name=name, attributes=attrs, **args)

    def push(self):
        '''Push object to API (PATCHes if self.Id is defined, else POSTs)'''
        if self.Id:
            return self.patch()
        else:
            return self.post()

    def get(self):
        '''Get object from API'''
        self.fromJson(req('GET', self._getEndpoint()))
        return self

    def post(self):
        '''Post new object to API'''
        self._validatePost()
        self.fromJson(req('POST', self._postEndpoint(), self.attrJson()))
        return self

    def patch(self):
        '''Update object on API'''
        self._validatePatch()
        self.fromJson(req('PATCH', self._postEndpoint(), self.attrJson()))
        return self

    def delete(self):
        '''You don't want to do this'''
        return self.Id and req('DELETE', self._postEndpoint())

    def diff(self, other=None):
        '''Diff this object and other, or API version if other is None'''
        if other is None:
            other = self.__class__(self.Id).get()
        elif not isinstance(other, self.__class__):
            raise(Exception('Objects must be of same class'))
        s1 = self.json(sort_keys=True, indent=0).splitlines(1)
        s2 = other.json(sort_keys=True, indent=0).splitlines(1)
        diff = difflib.unified_diff(s1, s2)
        print(''.join(diff))
        return diff


class Dataset(rwObj):
    '''Dataset object'''
    _ENDPOINT = 'dataset'

    def __init__(self, Id=None, name=None, apps=[], slug=None, provider=None,
                 connectorType=None, connectorUrl=None, tableName=None,
                 attributes=None):
        # Read only properties
        if attributes is None: attributes = {}
        if slug: attributes['slug'] = slug
        if provider: attributes['provider'] = provider
        if connectorType: attributes['connectorType'] = connectorType
        if connectorUrl: attributes['connectorUrl'] = connectorUrl
        if tableName: attributes['connectorType'] = tableName

        super(Dataset, self).__init__(Id, name, apps, attributes)

        self._layers = {}
        self._metadata = {}
        self._widgets = {}
        self._extractObjects()

    # Read only properties
    @property
    def tableName(self): return self.attributes['tableName']
    @property
    def connectorType(self): return self.attributes['connectorType']
    @property
    def connectorUrl(self): return self.attributes['connectorUrl']
    @property
    def status(self): return self.attributes['status']
    @property
    def Layers(self): return self._layers
    @property
    def Metadata(self): return self._metadata
    @property
    def Widgets(self): return self._widgets

    # Read-write properties
    @property
    def env(self): return self.attributes['env']
    @env.setter
    def env(self, env): self.attributes['env'] = env
    @property
    def subscribable(self): return self.attributes['subscribable']
    @subscribable.setter
    def subscribable(self, subs): self.attributes['subscribable'] = subs
    @property
    def mainDateField(self): return self.attributes['mainDateField']
    @mainDateField.setter
    def mainDateField(self, field): self.attributes['mainDateField'] = field

    # Methods
    def _validatePost(self):
        '''Check existance of required fields'''
        try:
            self.attributes and self.name and self.apps and self.connectorType
            if self.provider in ('gee', 'nexgddp'):
                self.tableName
            else:
                self.connectorUrl
        except AttributeError as e:
            logging.error("The following attributes must be defined: name, apps, provider, connectorUrl OR tableName (gee and nexgddp only)")
            raise(e)

    def _extractObjects(self):
        '''Convert json metadata, layers, widgets to objects'''
        if 'layer' in self.attributes:
            for lyr in self.attributes['layer']:
                self._layers[lyr['id']] = Layer(
                    lyr['id'], attributes=lyr['attributes'])
            del self.attributes['layer']
        if 'metadata' in self.attributes:
            for meta in self.attributes['metadata']:
                self._metadata[meta['id']] = Metadata(
                    meta['id'], attributes=meta['attributes'])
            del self.attributes['metadata']
        if 'widget' in self.attributes:
            for widget in self.attributes['widget']:
                self._widgets[widget['id']] = Widget(
                    widget['id'], attributes=widget['attributes'])
            del self.attributes['widget']

    def get(self, includes=[]):
        '''Get dataset definition from API'''
        params = ','.join(includes) if includes else None
        self._data = req('GET', self._getEndpoint(), params)
        self._extractObjects()
        return self

    def getLayers(self, limit=1000):
        '''Get assoctiated layers'''
        endpoint = urljoin(self._getEndpoint(), 'layer')
        params = {'page[size]':limit}
        data = req('GET', endpoint, params)
        for lyr in data:
            self._layers[lyr['id']] = Layer(
                lyr['id'], attributes=lyr['attributes'])

    def getMetadata(self, limit=1000):
        pass

    def getWidgets(self, limit=1000):
        pass


class Layer(rwObj):
    _GET_ENDPOINT = 'layer'
    _ENDPOINT = 'dataset/{datasetId}/layer'

    def __init__(self, Id=None, name=None, apps=[], datasetId=None, slug=None,
                 attributes=None):
        # Read only properties
        if attributes is None: attributes = {}
        if datasetId: attributes['dataset'] = datasetId
        if slug: slug['slug'] = slug
        self._dataset = None

        super(Layer, self).__init__(Id, name, apps, attributes)

    # Read only properties
    @property
    def datasetId(self): return self.attributes['dataset']
    @property
    def Dataset(self): return self._dataset

    # Read-write access properties
    @property
    def layerConfig(self): return self.attributes['layerConfig']['body']['layers'][0]
    @layerConfig.setter
    def layerConfig(self, config): self.attributes['layerConfig']['body']['layers'][0] = config
    @property
    def env(self): return self.attributes['env']
    @env.setter
    def env(self, env): self.attributes['env'] = env
    @property
    def published(self): return self.attributes['published']
    @published.setter
    def published(self, published): self.attributes['published'] = published
    @property
    def defaultLayer(self): return self.attributes['default']
    @defaultLayer.setter
    def defaultLayer(self, default): self.attributes['default'] = default

    # Private methods
    def _postEndpoint(self):
        if self.Id:
            return urljoin(self._ENDPOINT.format(datasetId=self.datasetId),
                                self.Id)
        return self._ENDPOINT.format(datasetId=self.datasetId)

    def _validatePost(self):
        try:
            self.attributes and self.apps and self.name and self.datasetId
        except AttributeError as e:
            logging.error("The following attributes must be defined: datasetId, name, apps")
            raise(e)

    def _validatePatch(self):
        try:
            self.attributes and self.apps and self.name and self.datasetId
        except AttributeError as e:
            logging.error("The following attributes must be defined: datasetId, name, apps")
            raise(e)

    # Public methods
    def getDataset(self, includes=[]):
        '''Get the dataset object that this layer belongs to'''
        self._dataset = Dataset(self.datasetId).get(includes)

        # add refrence to self in dataset
        self.Dataset.Layers[self.Id] = self
        return self.Dataset


# TODO
class Metadata(rwObj):
    _ENDPOINT = 'dataset/{datasetId}/metadata'
    _GET_ENDPOINT = 'metadata'
    pass

class Widget(rwObj):
    _ENDPOINT = 'dataset/{datasetId}/widget'
    _GET_ENDPOINT = 'widget'
    pass
