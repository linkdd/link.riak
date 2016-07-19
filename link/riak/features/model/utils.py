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


def model_save(model):
    model._map.key = model[model._DATA_ID] if model._DATA_ID in model else None

    try:
        model._middleware.conn.update_datatype(model._map)

    except ValueError:  # No operation to send
        pass

    else:
        model[model._DATA_ID] = model._map.key


def model_delete(model):
    model._middleware.conn.delete(model._map)

    if model._DATA_ID in model:
        del model[model._DATA_ID]


def create_model_class(name, bases, members):
    def model_init(model, model_map=None, *args, **kwargs):
        for base in bases:
            base.__init__(model, *args, **kwargs)

        if model_map is None:
            model_map = Map(
                bucket=model._middleware.conn.bucket_type(
                    '{0}s'.format(model._schemaname)
                ).bucket(
                    'default'
                )
            )

        model._map = model_map

    clsmembers = {
        '_DATA_ID': '_yz_rk',
        '__init__': model_init,
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