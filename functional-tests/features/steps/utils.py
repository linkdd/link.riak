# -*- coding: utf-8 -*-

from aloe import step, world

from link.kvstore.core import KeyValueStore
from b3j0f.utils.path import lookup
from time import sleep


# make sure driver is registered
lookup('link.riak.driver')


@step(r'I connect to "([^"]*)"')
def connect_to_kvstore(step, uri):
    world.store = KeyValueStore.get_middleware_by_uri(uri)


@step(r'I disconnect from the store')
def disconnect_from_kvstore(step):
    del world.store


@step(r'I wait (\d*) seconds')
def wait(step, seconds):
    sleep(int(seconds))
