# -*- coding: utf-8 -*-

from riak.datatypes import Register, Counter, Flag, Set
from b3j0f.utils.iterable import isiterable
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

    @property
    def dtkey(self):
        result = self.name

        if self.isregister:
            result = result[:-len('_register')]

        elif self.ismap:
            result = result[:-len('_map')]

        elif self.iscounter:
            result = result[:-len('_counter')]

        elif self.isflag:
            result = result[:-len('_flag')]

        elif self.isset:
            result = result[:-len('_set')]

        return result

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
                obj._map.registers[self.dtkey].assign(result)
                result = obj._map.registers[self.dtkey]

        elif self.ismap:
            pass

        elif self.iscounter:
            if not isinstance(result, Counter):
                if result >= 0:
                    obj._map.counters[self.dtkey].increment(result)

                else:
                    obj._map.counters[self.dtkey].decrement(-result)

                result = obj._map.counters[self.dtkey]

        elif self.isflag:
            if not isinstance(result, Flag):
                if result:
                    obj._map.flags[self.dtkey].enable()

                else:
                    obj._map.flags[self.dtkey].disable()

                result = obj._map.flags[self.dtkey]

        elif self.isset:
            if not isinstance(result, Set):
                for item in result:
                    obj._map.sets[self.dtkey].add(item)

                result = obj._map.sets[self.dtkey]

        return result

    def _fset(self, obj, val):
        val = self.convert_value(val)

        if self.is_datatype:
            dt = self._fget(obj)

            if self.isregister:
                dt.assign(val)

            elif self.iscounter:
                if val >= 0:
                    dt.increment(val)

                else:
                    dt.decrement(-val)

            elif self.isflag:
                if val:
                    dt.enable()

                else:
                    dt.disable()

            elif self.isset:
                for item in val:
                    if item[0] == '-':
                        dt.discard(item[1:])

                    else:
                        if item[0] == '+':
                            item = item[1:]

                        dt.add(item)

            elif self.ismap:
                raise AttributeError('Cannot set map attributes')

            val = dt

        setattr(obj, self.attr, val)

    def _fdel(self, obj):
        if self.is_datatype:
            if self.ismap:
                obj._map.maps[self.dtkey].delete()

            else:
                dt = self._fget(obj)
                dt.delete()

        else:
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
        result = self.cls(
            obj._middleware, '',
            model_map=obj._map.maps[self.dtkey]
        )
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

    class array(list):
        def append(self, item):
            item = self.__subtype__.convert_value(item)
            return super(ArrayField.array, self).append(item)

        def extend(self, l):
            L = [
                self.__subtype__.convert_value(item)
                for item in l
            ]

            return super(ArrayField.array, self).extend(L)

        def insert(self, i, item):
            item = self.__subtype__.convert_value(item)

            return super(ArrayField.array, self).insert(i, item)

        def remove(self, item):
            item = self.__subtype__.convert_value(item)
            return super(ArrayField.array, self).remove(item)

        def __setitem__(self, i, item):
            item = self.__subtype__.convert_value(item)

            return super(ArrayField.array, self).__setitem__(i, item)

    def __init__(self, subtype, *args, **kwargs):
        super(ArrayField, self).__init__(*args, **kwargs)

        self.subtype = subtype(self.attr)

    def override_list(self, val):
        val.__class__ = ArrayField.array
        val.__subtype__ = self.subtype

    def setdefault(self, obj):
        result = ArrayField.array()
        result.__subtype__ = self.subtype

        self.override_list(result)
        setattr(obj, self.attr, result)
        return result

    def convert_value(self, val):
        if not isiterable(val, exclude=string_types):
            val = [val]

        if not isinstance(val, ArrayField.array):
            result = ArrayField.array(val)
            result.__subtype__ = self.subtype

        return result
