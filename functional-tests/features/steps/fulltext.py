# -*- coding: utf-8 -*-

from aloe.tools import guess_types
from aloe import step, world

from link.feature import getfeature
from subprocess import call
from riak import RiakError
import json


@step(r'I can index riak with the schema "([^"]*)"')
def create_index(step, schema):
    ret = 0

    ret = call(
        [
            'sudo',
            'riak-admin',
            'bucket-type',
            'create',
            'fulltext',
            json.dumps({'props': {}})
        ]
    )

    if ret == 0:
        ret = call(
            [
                'sudo',
                'riak-admin',
                'bucket-type',
                'activate',
                'fulltext'
            ]
        )

    assert ret == 0

    riakmiddleware = world.store.get_child_middleware()
    riakclient = riakmiddleware.conn

    with open(schema) as f:
        riakclient.create_search_schema('fulltext', f.read())

    riakclient.create_search_index('fulltext', 'fulltext')

    try:
        index = riakclient.get_search_index('fulltext')

    except RiakError:
        index = {'name': None}

    assert index['name'] == 'fulltext'

    ret = call(
        [
            'sudo',
            'riak-admin',
            'bucket-type',
            'update',
            'fulltext',
            json.dumps({'props': {'search_index': 'fulltext'}})
        ]
    )

    assert ret == 0


@step(r'I use the feature "([^"]*)"')
def use_feature(step, featurename):
    world.feature = getfeature(world.store, featurename)


@step(r'I have the following entries:')
def feed_initial_data(step):
    for entry in guess_types(step.hashes):
        key = entry['Key']
        doc = {
            'field1': entry['Field1'],
            'field2': entry['Field2']
        }

        world.store[key] = doc


@step(r'I can search for "([^"]*)" and find the keys:')
def query_index(step, search):
    result = world.feature.search(search)

    riak_keys = [
        doc[world.feature.DATA_ID]
        for doc in result
    ]

    expected = [
        item['Key']
        for item in guess_types(step.hashes)
    ]

    assert len(expected) == len(riak_keys)

    for key in expected:
        assert key in riak_keys
