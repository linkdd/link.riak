# -*- coding: utf-8 -*-

from b3j0f.conf import Configurable

from xml.etree import ElementTree as ET
from copy import deepcopy

from link.riak.features.model.utils import create_model_class
from link.riak.features.model.utils import get_member
from link.riak.features.model.utils import get_type
from link.model.feature import ModelFeature, Model
from link.riak import CONF_BASE_PATH


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
            typecls = get_type(_type.get('class'))
            typeattr = deepcopy(_type.attrib)

            typeattr.pop('name')
            typeattr.pop('class')

            typemapping[typename] = (typecls, typeattr)

        for field in root.iter('field'):
            fname = field.get('name')
            fnames = fname.split('.')

            if len(fnames) == 1:
                clsmembers[fname] = get_member(typemapping, field)

            else:
                tree = clsmembers

                for name in fnames[:-1]:
                    if name not in tree:
                        tree[name] = {}

                    tree = tree[name]

                tree[fnames[-1]] = get_member(typemapping, field)

        return create_model_class(clsname, clsbases, clsmembers)
