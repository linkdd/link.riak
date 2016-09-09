# -*- coding: utf-8 -*-

from aloe.tools import guess_types
from aloe import step, world

from link.crdt.counter import Counter
from link.crdt.set import Set
from link.crdt.map import Map
from subprocess import call
import json


def create_bucket_type(btype):
    bucket_name = '{0}s'.format(btype)
    ret = call(
        [
            'sudo',
            'riak-admin',
            'bucket-type',
            'create',
            bucket_name,
            json.dumps({'props': {'datatype': btype}})
        ]
    )

    if ret == 0:
        return call(
            [
                'sudo',
                'riak-admin',
                'bucket-type',
                'activate',
                bucket_name
            ]
        )


@step(r'I make sure riak can store counters')
def ensure_counter_datatype(step):
    assert create_bucket_type('counter') == 0


@step(r'I make sure riak can store sets')
def ensure_set_datatype(step):
    assert create_bucket_type('set') == 0


@step(r'I make sure riak can store maps')
def ensure_map_datatype(step):
    assert create_bucket_type('map') == 0


@step(r'I create a counter "([^"]*)" starting at (\d*)')
def create_counter(step, key, val):
    counter = Counter()
    counter.increment(int(val))
    world.store[key] = counter


@step(r'I increment the counter "([^"]*)" by (\d*)')
def update_counter(step, key, val):
    counter = world.store[key]
    counter.increment(int(val))
    world.store[key] = counter


@step(r'I have a counter "([^"]*)" starting at (\d*)')
def read_counter(step, key, val):
    counter = world.store[key]
    assert counter.current == int(val)


@step(r'I create a set "([^"]*)" containing (?:"([^"]*)"(?: and "([^"]*)")*)')
def create_set(step, key, *vals):
    s = Set()

    for val in vals:
        if val is not None:
            s.add(val)

    world.store[key] = s


@step(r'I add "([^"]*)" to the set "([^"]*)"')
def add_to_set(step, val, key):
    s = world.store[key]
    s.add(val)
    world.store[key] = s


@step(r'I discard "([^"]*)" from the set "([^"]*)"')
def del_from_set(step, val, key):
    s = world.store[key]
    s.discard(val)
    world.store[key] = s


@step(r'I have a set "([^"]*)" containing (?:"([^"]*)"(?: and "([^"]*)")*)')
def read_set(step, key, *vals):
    s = world.store[key]

    for val in vals:
        if val is not None:
            assert val in s


@step(
    r'I create a map "([^"]*)" containing a flag "([^"]*)" enabled by default'
)
def create_map(step, key, flagkey):
    m = Map()
    m['{0}_flag'.format(flagkey)].enable()
    world.store[key] = m


@step(r'I increment a counter "([^"]*)" by (\d*) in the map "([^"]*)"')
def update_map_counter(step, counterkey, val, key):
    m = world.store[key]
    m['{0}_counter'.format(counterkey)].increment(int(val))
    world.store[key] = m


@step(r'I set a register "([^"]*)" to "([^"]*)" in the map "([^"]*)"')
def update_map_register(step, regkey, val, key):
    m = world.store[key]
    m['{0}_register'.format(regkey)].assign(val)
    world.store[key] = m


@step(r'I add to a set "([^"]*)" in the map "([^"]*)":')
def update_map_set(step, setkey, key):
    m = world.store[key]
    setkey = '{0}_set'.format(setkey)

    for value in guess_types(step.hashes):
        m[setkey].add(value['Value'])

    world.store[key] = m


@step(
    r'I add to a map "([^"]*)" a counter "([^"]*)"' +
    r' incremented by (\d*) in the map "([^"]*)"'
)
def update_map_map(step, mapkey, counterkey, val, key):
    m = world.store[key]
    mapkey = '{0}_map'.format(mapkey)
    counterkey = '{0}_counter'.format(counterkey)
    m[mapkey][counterkey].increment(int(val))
    world.store[key] = m


@step(r'I have a map "([^"]*)" which match the value in "([^"]*)"')
def read_map(step, key, path):
    with open(path) as f:
        expected = json.load(f)

    m = world.store[key]

    for key in expected:
        assert key in m.current

        if isinstance(expected[key], list):
            for item in expected[key]:
                assert item in m.current[key]

        else:
            assert expected[key] == m.current[key]
