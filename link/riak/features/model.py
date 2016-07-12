# -*- coding: utf-8 -*-

from b3j0f.conf import Configurable
from xml.etree import ElementTree as ET
from six import string_types
from copy import deepcopy

from link.model.feature import ModelFeature, Model
from link.riak import CONF_BASE_PATH


class solr:
    class BaseField(property):
        def __init__(self, attributes, *args, **kwargs):
            kwargs['fget'] = self._fget
            kwargs['fset'] = self._fset
            kwargs['fdel'] = self._fdel

            super(solr.BaseField, self).__init__(*args, **kwargs)

            self._attr = attributes
            self._value = None

        def _fget(self):
            raise NotImplementedError()

        def _fset(self):
            raise NotImplementedError()

        def _fdel(self):
            raise NotImplementedError()

    class TrieIntField(BaseField):
        def _fget(self):
            return self._value

        def _fset(self, val):
            if not isinstance(val, int):
                val = int(val)

            self._value = val

    class BoolField(BaseField):
        def _fget(self):
            return self._value

        def _fset(self, val):
            if not isinstance(val, bool):
                val = (val == 'true')

            self._value = val

    class StrField(BaseField):
        def _fget(self):
            return self._value

        def _fset(self, val):
            if not isinstance(val, string_types):
                val = str(val)

            self._value = val


@Configurable(paths='{0}/model.conf'.format(CONF_BASE_PATH))
class RiakSolrSchema(ModelFeature):

    DATA_ID = '_yz_rk'

    def create_model(self, schema):
        root = ET.fromstring(schema)

        clsname = root.get('name')
        clsbases = (Model,)
        clsmembers = {}

        typemapping = {}

        for _type in root.iter('fieldType'):
            typename = _type.get('name')
            typecls = getattr(solr, _type.get('class')[len('solr.'):])
            typeattr = deepcopy(_type.attrib)

            typeattr.pop('name')
            typeattr.pop('class')

            typemapping[typename] = (typecls, typeattr)

        for field in root.iter('field'):
            fname = field.get('name')
            ftype, typeattr = typemapping.get(field.get('type'))
            fattr = deepcopy(field.attrib)
            fattr.update(typeattr)

            clsmembers[fname] = ftype(fattr)

        return type(clsname, clsbases, clsmembers)
