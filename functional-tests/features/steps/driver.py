# -*- coding: utf-8 -*-

from aloe import step, world
import json


@step(r'I create a key "([^"]*)" with the JSON document "([^"]*)"')
def create_key(step, key, document):
    with open(document) as f:
        document = json.load(f)

    world.store[key] = document


@step(r'I can get the key "([^"]*)" which match the value in "([^"]*)"')
def read_key(step, key, document):
    with open(document) as f:
        document = json.load(f)

    result = world.store[key]

    assert result == document


@step(r'I can update the key "([^"]*)" with the JSON document "([^"]*)"')
def update_key(step, key, document):
    with open(document) as f:
        document = json.load(f)

    world.store[key] = document


@step(r'I can delete the key "([^"]*)"')
def delete_key(step, key):
    del world.store[key]


@step(r'I can find the key "([^"]*)"')
def find_key(step, key):
    assert key in world.store


@step(r'I cannot find the key "([^"]*)"')
def do_not_find_key(step, key):
    assert key not in world.store
