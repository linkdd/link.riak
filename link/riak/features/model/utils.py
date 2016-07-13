# -*- coding: utf-8 -*-

from b3j0f.utils.path import lookup
from riak.datatypes import Map
from copy import deepcopy


def get_type(name):
    return lookup('link.riak.features.model.{0}'.format(name))


def get_member(typemapping, field):
    ftype, typeattr = typemapping.get(field.get('type'))
    fattr = deepcopy(field.attrib)
    fattr.update(typeattr)

    isarray = (field.get('multiValued', 'false') == 'true')

    if isarray:
        ArrayField = get_type('solr.ArrayField')
        member = ArrayField(ftype, fattr)

    else:
        member = ftype(fattr)

    return member


def model_to_map(model, dt=None):
    if dt is None:
        dt = Map()

    for key in model:
        if key.endswith('_register'):
            dtkey = key[:-len('_register')]
            dt.registers[dtkey] = model[key]

        elif key.endswith('_set'):
            dtkey = key[:-len('_set')]
            dt.sets[dtkey] = model[key]

        elif key.endswith('_flag'):
            dtkey = key[:-len('_flag')]
            dt.flags[dtkey] = model[key]

        elif key.endswith('_counter'):
            dtkey = key[:-len('_counter')]
            dt.counters[dtkey] = model[key]

        elif key.endswith('_map'):
            dtkey = key[:-len('_map')]
            dt.maps[dtkey] = model[key].to_map()

    return dt


def model_save(model):
    riak_map = model.to_map()
    riak_map = model._middleware.conn.update_datatype(riak_map)
    model.to_map(riak_map)


def model_delete(model):
    riak_map = model.to_map()
    model._middleware.conn.delete(riak_map)


def create_model_class(name, bases, members):
    clsmembers = {
        '_DATA_ID': '_yz_rk',
        'to_map': model_to_map,
        'save': model_save,
        'delete': model_delete
    }

    for mname in members:
        member = members[mname]

        if isinstance(member, dict):
            EmbeddedModelField = get_type('solr.EmbeddedModelField')

            member = EmbeddedModelField(
                member,
                {
                    'name': mname
                }
            )

        clsmembers[mname] = member

    return type(name, bases, clsmembers)
