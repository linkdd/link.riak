# -*- coding: utf-8 -*-

from b3j0f.utils.iterable import isiterable
from b3j0f.aop import weave, get_advices
from b3j0f.conf import Configurable

from xml.etree import ElementTree as ET
from six import string_types
from copy import deepcopy

from link.model.feature import ModelFeature, Model
from link.riak import CONF_BASE_PATH


class solr:
    class BaseField(property):
        @property
        def name(self):
            return self._attr['name']

        @property
        def attr(self):
            return '_{0}'.format(self._attr['name'])

        def __init__(self, attributes, *args, **kwargs):
            kwargs['fget'] = self._fget
            kwargs['fset'] = self._fset
            kwargs['fdel'] = self._fdel

            super(solr.BaseField, self).__init__(*args, **kwargs)

            self._attr = attributes

        def convert_value(self, val):
            raise NotImplementedError()

        def setdefault(self, obj):
            raise NotImplementedError()

        def _fget(self, obj):
            try:
                result = getattr(obj, self.attr)

            except AttributeError:
                result = self.setdefault(obj)

            return result

        def _fset(self, obj, val):
            val = self.convert_value(val)
            setattr(obj, self.attr, val)

        def _fdel(self, obj):
            delattr(obj, self.attr)

    class TrieIntField(BaseField):
        def convert_value(self, val):
            if not isinstance(val, int):
                val = int(val)

            return val

        def setdefault(self, obj):
            result = 0
            setattr(obj, self.attr, result)
            return result

    class BoolField(BaseField):
        def convert_value(self, val):
            if not isinstance(val, bool):
                val = (val == 'true')

            return val

        def setdefault(self, obj):
            result = False
            setattr(obj, self.attr, result)
            return result

    class StrField(BaseField):
        def convert_value(self, val):
            if not isinstance(val, string_types):
                val = str(val)

            return val

        def setdefault(self, obj):
            result = ''
            setattr(obj, self.attr, result)
            return result

    class ArrayField(BaseField):
        def __init__(self, subtype, *args, **kwargs):
            super(solr.ArrayField, self).__init__(*args, **kwargs)

            self.subtype = subtype(self.attr)

        def ensure_type_append(self, jointpoint):
            args = list(jointpoint.args)
            args[0] = self.subtype.convert_value(args[0])
            jointpoint.args = tuple(args)

            return jointpoint.proceed()

        def ensure_type_extend(self, jointpoint):
            L = jointpoint.args[0]

            for i, item in enumerate(L):
                L[i] = self.subtype.convert_value(item)

            return jointpoint.proceed()

        def ensure_type_insert(self, jointpoint):
            args = list(jointpoint)
            args[1] = self.subtype.convert_value(args[1])
            jointpoint.args = tuple(args)

            return jointpoint.proceed()

        def weave_list(self, val):
            methods = [
                (val.append, self.ensure_type_append),
                (val.extend, self.ensure_type_extend),
                (val.insert, self.ensure_type_insert),
                (val.remove, self.ensure_type_append),
                (val.__setitem__, self.ensure_type_insert)
            ]

            for method, advice in methods:
                if advice not in get_advices(method):
                    weave(target=method, advices=advice)

        def setdefault(self, obj):
            result = []
            self.weave_list(result)
            setattr(obj, self.attr, result)
            return result

        def convert_value(self, val):
            if not isiterable(val, exclude=string_types):
                val = [val]

            self.weave_list(val)

            return val


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

            isarray = (field.get('multiValued', 'false') == 'true')

            if isarray:
                clsmembers[fname] = solr.ArrayField(ftype, attr)

            else:
                clsmembers[fname] = ftype(fattr)

        return type(clsname, clsbases, clsmembers)
