# -*- coding: utf-8 -*-

from aloe import step, world

from link.crdt.counter import Counter
from link.crdt.set import Set
from subprocess import call
import json


def create_bucket_type(btype):
    bucket_name = '{0}s'.format(btype)
    ret = call([
        'sudo',
        'riak-admin',
        'bucket-type',
        'create',
        bucket_name,
        json.dumps({'props': {'datatype': btype}})
    ])

    if ret == 0:
        return call([
            'sudo',
            'riak-admin',
            'bucket-type',
            'activate',
            bucket_name
        ])


@step(r'I make sure riak can store counters')
def ensure_counter_datatype(step):
    assert create_bucket_type('counter') == 0


@step(r'I make sure riak can store sets')
def ensure_set_datatype(step):
    assert create_bucket_type('set') == 0


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
