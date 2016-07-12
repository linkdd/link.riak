# -*- coding: utf-8 -*-

from riak.datatypes import Register, Map, Counter, Flag, Set
from b3j0f.utils.iterable import isiterable
from b3j0f.aop import weave, get_advices
from six import string_types

from link.riak.features.model.utils import create_model_class
from link.model.feature import Model


class BaseField(property):
    @property
    def name(self):
        return self._attr['name']

    @property
    def attr(self):
        return '_{0}'.format(self._attr['name'])

    @property
    def isregister(self):
        return self.name.endswith('_register')

    @property
    def ismap(self):
        return self.name.endswith('_map')

    @property
    def iscounter(self):
        return self.name.endswith('_counter')

    @property
    def isflag(self):
        return self.name.endswith('_flag')

    @property
    def isset(self):
        return self.name.endswith('_set')

    @property
    def is_datatype(self):
        return any([
            self.isregister,
            self.ismap,
            self.iscounter,
            self.isflag,
            self.isset
        ])

    def __init__(self, attributes, *args, **kwargs):
        kwargs['fget'] = self._fget
        kwargs['fset'] = self._fset
        kwargs['fdel'] = self._fdel

        super(BaseField, self).__init__(*args, **kwargs)

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

        if self.isregister:
            if not isinstance(result, Register):
                result = Register(value=result)

        elif self.ismap:
            if not isinstance(result, Map):
                result = result.to_map()

        elif self.iscounter:
            if not isinstance(result, Counter):
                result = Counter(value=result)

        elif self.isflag:
            if not isinstance(result, Flag):
                result = Flag(value=result)

        elif self.isset:
            if not isinstance(result, Set):
                result = Set(value=frozenset(result))

        return result

    def _fset(self, obj, val):
        val = self.convert_value(val)

        if self.is_datatype:
            dt = self._fget(obj)

            if self.isregister:
                dt.assign(val)

            elif self.iscounter:
                v = val - dt.value

                if v >= 0:
                    dt.increment(v)

                else:
                    dt.decrement(v)

            elif self.isflag:
                if v:
                    dt.enable()

                else:
                    dt.disable()

            elif self.isset:
                for item in val:
                    if item not in dt.value:
                        dt.add(item)

                    else:
                        dt.discard(item)

            elif self.ismap:
                val.to_map(dt)

        else:
            setattr(obj, self.attr, val)

    def _fdel(self, obj):
        delattr(obj, self.attr)


class EmbeddedModelField(BaseField):
    def __init__(self, members, *args, **kwargs):
        super(EmbeddedModelField, self).__init__(*args, **kwargs)

        self.cls = create_model_class(
            'EmbeddedModel_{0}'.format(self.name),
            (Model,),
            members
        )

    def convert_value(self, val):
        if not isinstance(val, self.cls):
            raise TypeError('{0} must be an embedded model: {1}'.format(
                self.name,
                self.cls.__name__
            ))

        return val

    def setdefault(self, obj):
        result = self.cls(obj.middleware)
        setattr(obj, self.attr, result)
        return result


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
        super(ArrayField, self).__init__(*args, **kwargs)

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
