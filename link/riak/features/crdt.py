# -*- coding: utf-8 -*-

from riak.datatypes.datatype import TYPES as RIAK_TYPES
from link.crdt.map import TYPES as LINK_TYPES


class CRDTConverter(object):
    def new_riak_crdt(self, crdt, value, context=None):
        return RIAK_TYPES[crdt._type_name](value=value, context=context)

    def new_link_crdt(self, crdt, value, context=None):
        return LINK_TYPES[crdt.type_name](value=value, context=context)

    def to_counter(self, crdt, context=None):
        obj = self.new_riak_crdt(crdt, crdt._value, context=context)
        obj._increment = crdt._increment
        return obj

    def from_counter(self, crdt, context=None):
        obj = self.new_link_crdt(crdt, crdt.value, context=context)
        obj._increment = crdt._increment
        return obj

    def to_flag(self, crdt, context=None):
        obj = self.new_riak_crdt(crdt, crdt._value, context=context)
        obj._op = str(crdt._mutation) if crdt._mutation is not None else None
        return obj

    def from_flag(self, crdt, context=None):
        obj = self.new_link_crdt(crdt, crdt.value, context=context)
        obj._mutation = crdt._op
        return obj

    def to_set(self, crdt, context=None):
        obj = self.new_riak_crdt(crdt, crdt._value, context=context)
        obj._adds = set(map(str, crdt._adds))
        obj._removes = set(map(str, crdt._removes))
        return obj

    def from_set(self, crdt, context=None):
        obj = self.new_link_crdt(crdt, crdt.value, context=context)
        obj._adds = crdt._adds
        obj._removes = crdt._removes
        return obj

    def to_register(self, crdt, context=None):
        obj = self.new_riak_crdt(crdt, crdt._value, context=context)
        obj._new_value = str(crdt._new) if crdt._new is not None else None
        return obj

    def from_register(self, crdt, context=None):
        obj = self.new_link_crdt(crdt, crdt.value, context=context)
        obj._new = crdt._new_value
        return obj

    def to_map(self, crdt, context=None):
        obj = self.new_riak_crdt(crdt, {}, context=context)

        val = {}

        for key in crdt._value:
            subval = self.to_riak(crdt._value[key], context=obj)
            key, datatype = str(key).rsplit('_', 1)

            val[(key, datatype)] = subval

        obj._value = val

        updates = {}

        for key in crdt._updates:
            subval = self.to_riak(crdt._updates[key], context=obj)
            key, datatype = str(key).rsplit('_', 1)

            updates[(key, datatype)] = subval

        obj._removes = []

        for key in crdt._removes:
            key, datatype = str(key).rsplit('_', 1)
            obj._removes.append((key, datatype))

        obj._updates = updates

        return obj

    def from_map(self, crdt, context=None):
        obj = self.new_link_crdt(crdt, {}, context=context)

        val = {}

        for key, datatype in crdt._value:
            subval = self.from_riak(
                crdt._value[(key, datatype)],
                context=obj
            )
            key = '{0}_{1}'.format(key, datatype)

            val[key] = subval

        obj._value = val

        updates = {}

        for key, datatype in crdt._updates:
            subval = self.from_riak(
                crdt._updates[(key, datatype)],
                context=obj
            )
            key = '{0}_{1}'.format(key, datatype)

            updates[key] = subval

        obj._removes = [
            '{0}_{1}'.format(key, datatype)
            for key, datatype in crdt._removes
        ]

        obj._updates = updates

        return obj

    def to_riak(self, crdt, context=None):
        typemap = {
            'counter': self.to_counter,
            'flag': self.to_flag,
            'set': self.to_set,
            'register': self.to_register,
            'map': self.to_map
        }

        return typemap[crdt._type_name](crdt, context=context)

    def from_riak(self, crdt, context=None):
        typemap = {
            'counter': self.from_counter,
            'flag': self.from_flag,
            'set': self.from_set,
            'register': self.from_register,
            'map': self.from_map
        }

        return typemap[crdt.type_name](crdt, context=context)


def convert_crdt_to_riak(crdt):
    converter = CRDTConverter()
    return converter.to_riak(crdt)


def convert_crdt_from_riak(riak_crdt):
    converter = CRDTConverter()
    return converter.from_riak(riak_crdt)
