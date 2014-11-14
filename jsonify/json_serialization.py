__author__ = 'filip'

import json
from abc import ABCMeta, abstractmethod

class JsonSerializer(json.JSONEncoder):
    def default(self, o):
        d = {
            '__class__': o.__class__.__name__,
            '__module__': o.__module__,
        }
        return d.update(o.__dict__)


class JsonDeserializer(json.JSONDecoder):

    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object)

    @staticmethod
    def dict_to_object(d):
        if hasattr(d, '__class__') and hasattr(d, '__module__'):
            class_name = d.get('__class__')
            module_name = d.get('__module__')
            module = __import__(module_name)
            clazz = getattr(module, class_name)
            kwargs = dict((key.encode('ascii'), value) for key, value in d.items())
            inst = clazz(**kwargs)
        else:
            inst = dict(d)
        return inst


class JsonObj(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def as_json(self):
        pass


class JsonTypedDeserializer(json.JSONDecoder):
    def __init__(self, clazz=dict):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object)
        self._clazz = clazz

    def set_class(self, clazz):
        self._clazz = clazz

    def get_class(self):
        return self._clazz

    def dict_to_object(self, d):
        kwargs = dict((key.encode('ascii'), value) for key, value in d.items())
        return self._clazz(**kwargs)