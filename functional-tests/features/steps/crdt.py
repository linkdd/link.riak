# -*- coding: utf-8 -*-

from aloe import step, world

from link.crdt.counter import Counter
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


@step(r'I create a key "([^"]*)" with a counter starting at (\d*)')
def create_counter(step, key, val):
    counter = Counter()
    counter.increment(int(val))
    world.store[key] = counter


@step(r'I make sure it can store counters')
def ensure_counter_datatype(step):
    assert create_bucket_type('counter') == 0


@step(r'I increment the counter "([^"]*)" by (\d*)')
def update_counter(step, key, val):
    counter = world.store[key]
    counter.increment(int(val))
    world.store[key] = counter


@step(r'I have a counter "([^"]*)" starting at (\d*)"')
def read_counter(step, key, val):
    counter = world.store[key]
    assert counter.current == int(val)
