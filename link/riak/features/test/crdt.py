# -*- coding: utf-8 -*-

from b3j0f.utils.ut import UTCase
from unittest import main

from link.crdt.map import TYPES

from link.riak.features.crdt import convert_crdt_from_riak
from link.riak.features.crdt import convert_crdt_to_riak


class TestCRDT(UTCase):
    def test_convert_counter(self):
        initial = TYPES['counter']()
        initial.increment()

        riak = convert_crdt_to_riak(initial)
        self.assertEqual(riak._increment, 1)

        crdt = convert_crdt_from_riak(riak)
        self.assertEqual(crdt._increment, 1)

    def test_convert_flag(self):
        initial = TYPES['flag']()
        initial.enable()

        riak = convert_crdt_to_riak(initial)
        self.assertEqual(riak._op, 'enable')

        crdt = convert_crdt_from_riak(riak)
        self.assertEqual(crdt._mutation, 'enable')

    def test_convert_set(self):
        initial = TYPES['set']()
        initial.add('hello')
        initial.add('world')
        initial.discard('hello')

        riak = convert_crdt_to_riak(initial)
        self.assertEqual(riak._adds, {'hello', 'world'})
        self.assertEqual(riak._removes, {'hello'})

        crdt = convert_crdt_from_riak(riak)
        self.assertEqual(crdt._adds, {'hello', 'world'})
        self.assertEqual(crdt._removes, {'hello'})

    def test_convert_register(self):
        initial = TYPES['register']()
        initial.assign('foo')

        riak = convert_crdt_to_riak(initial)
        self.assertEqual(riak._new_value, 'foo')

        crdt = convert_crdt_from_riak(riak)
        self.assertEqual(crdt._new, 'foo')

    def test_convert_map(self):
        initial = TYPES['map'](value={
            'g_flag': False
        })

        initial['a_counter'].increment()
        initial['b_flag'].enable()
        initial['c_set'].add('hello')
        initial['c_set'].add('world')
        initial['c_set'].discard('hello')
        initial['d_register'].assign('foo')
        initial['e_map']['a_counter'].increment()
        initial['f_counter'].increment()
        del initial['f_counter']

        riak = convert_crdt_to_riak(initial)

        keys = [
            ('a', 'counter'),
            ('b', 'flag'),
            ('c', 'set'),
            ('d', 'register'),
            ('e', 'map'),
            ('f', 'counter')
        ]

        for key in keys:
            self.assertIn(key, riak._updates)

        self.assertIn(('f', 'counter'), riak._removes)
        self.assertIn(('g', 'flag'), riak)
        self.assertEqual(riak[('g', 'flag')].value, False)

        self.assertEqual(riak._updates[('a', 'counter')]._increment, 1)
        self.assertEqual(riak._updates[('b', 'flag')]._op, 'enable')
        self.assertEqual(riak._updates[('c', 'set')]._adds, {'hello', 'world'})
        self.assertEqual(riak._updates[('c', 'set')]._removes, {'hello'})
        self.assertEqual(riak._updates[('d', 'register')]._new_value, 'foo')

        self.assertIn(('a', 'counter'), riak._updates[('e', 'map')]._updates)
        self.assertEqual(
            riak._updates[('e', 'map')]._updates[('a', 'counter')]._increment,
            1
        )

        crdt = convert_crdt_from_riak(riak)

        keys = [
            'a_counter',
            'b_flag',
            'c_set',
            'd_register',
            'e_map',
            'f_counter'
        ]

        for key in keys:
            self.assertIn(key, crdt._updates)

        self.assertIn('f_counter', crdt._removes)
        self.assertIn('g_flag', crdt)
        self.assertEqual(crdt['g_flag'].current, False)

        self.assertEqual(crdt._updates['a_counter']._increment, 1)
        self.assertEqual(crdt._updates['b_flag']._mutation, 'enable')
        self.assertEqual(crdt._updates['c_set']._adds, {'hello', 'world'})
        self.assertEqual(crdt._updates['c_set']._removes, {'hello'})
        self.assertEqual(crdt._updates['d_register']._new, 'foo')

        self.assertIn('a_counter', crdt._updates['e_map']._updates)
        self.assertEqual(
            crdt._updates['e_map']._updates['a_counter']._increment,
            1
        )


if __name__ == '__main__':
    main()
