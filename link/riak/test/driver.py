# -*- coding: utf-8 -*-

from b3j0f.utils.ut import UTCase
from mock import MagicMock, patch
from unittest import main

from riak.datatypes.map import Map as RiakMap
from riak import RiakObject

from link.riak.driver import RiakDriver
from link.crdt.map import Map


class TestDriver(UTCase):
    def setUp(self):
        patcher1 = patch('link.riak.driver.SecurityCreds')
        patcher2 = patch('link.riak.driver.RiakClient')

        self.SecurityCreds = patcher1.start()
        self.RiakClient = patcher2.start()

        self.addCleanup(patcher1.stop)
        self.addCleanup(patcher2.stop)

        self.security = MagicMock()
        self.SecurityCreds.return_value = self.security

        self.conn = MagicMock()
        self.RiakClient.return_value = self.conn

        # middleware configuration
        self.drvconfig = {
            'hosts': [('localhost', 9000)],
            'user': 'guest',
            'pwd': 'guest',
            'default_bucket': 'testbucket',
            'protocol': 'http',
            'pkey': 'private key',
            'cert': 'certificate',
            'cacert': 'certificate authority',
            'crl': 'revocation list',
            'ciphers': 'ssl ciphers',
            'sslver': 5
        }

    def _instanciate(self, protocol='http'):
        self.drvconfig['protocol'] = protocol

        self.drv = RiakDriver(**self.drvconfig)

    def test_connect_nosecurity(self):
        self.drvconfig = {
            'hosts': [('localhost', 9000)]
        }
        self._instanciate()

        self.assertIs(self.conn, self.drv.conn)
        self.SecurityCreds.assert_not_called()
        self.RiakClient.assert_called_with(
            nodes=[
                {
                    'host': 'localhost',
                    'http_port': 9000
                }
            ],
            credentials=None
        )

    def test_connect_http(self):
        self._instanciate(protocol='http')

        self.assertIs(self.conn, self.drv.conn)

        self.SecurityCreds.assert_called_with(
            username='guest',
            password='guest',
            pkey_file='private key',
            cert_file='certificate',
            cacert_file='certificate authority',
            crl_file='revocation list',
            ciphers='ssl ciphers',
            ssl_version=5
        )
        self.RiakClient.assert_called_with(
            nodes=[
                {
                    'host': 'localhost',
                    'http_port': 9000
                }
            ],
            credentials=self.security
        )

    def test_connect_pbc(self):
        self._instanciate(protocol='pbc')

        self.assertIs(self.conn, self.drv.conn)

        self.SecurityCreds.assert_called_with(
            username='guest',
            password='guest',
            pkey_file='private key',
            cert_file='certificate',
            cacert_file='certificate authority',
            crl_file='revocation list',
            ciphers='ssl ciphers',
            ssl_version=5
        )
        self.RiakClient.assert_called_with(
            nodes=[
                {
                    'host': 'localhost',
                    'pb_port': 9000
                }
            ],
            credentials=self.security
        )

    def test_disconnect(self):
        self._instanciate()
        self.drv.connect()

        self.conn.is_alive.return_value = True
        self.assertTrue(self.drv.isconnected())

        self.drv.disconnect()

        self.conn.is_alive.return_value = False
        self.assertFalse(self.drv.isconnected())

        self.conn.close.assert_called_with()

    def test_default_bucket(self):
        bucket = 'expected'

        self.conn.bucket.return_value = bucket
        self._instanciate()
        self.drv.connect()

        result = self.drv._get_bucket(self.drv.conn)

        self.assertEqual(result, bucket)
        self.conn.bucket.assert_called_with('testbucket')

    def test_bucket(self):
        bucket = 'expected'

        self.conn.bucket.return_value = bucket

        self.drvconfig['path'] = ['testbucket2']
        self._instanciate()
        self.drv.connect()

        result = self.drv._get_bucket(self.drv.conn)

        self.assertEqual(result, bucket)
        self.conn.bucket.assert_called_with('testbucket2')

    def test_typed_bucket(self):
        bucket = 'expected'

        bucket_type = MagicMock()
        bucket_type.bucket.return_value = bucket
        self.conn.bucket_type.return_value = bucket_type

        self.drvconfig['path'] = ['testtype', 'testbucket']
        self._instanciate()
        self.drv.connect()

        result = self.drv._get_bucket(self.drv.conn)

        self.assertEqual(result, bucket)
        self.conn.bucket_type.assert_called_with('testtype')
        bucket_type.bucket.assert_called_with('testbucket')

    def test_get(self):
        obj = MagicMock()
        obj.__class__ = RiakObject
        obj.exists = True
        obj.data = 'expected'

        bucket = MagicMock()
        bucket.get.return_value = obj
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        result = self.drv._get(self.drv.conn, 'key')

        self.assertEqual(result, 'expected')
        bucket.get.assert_called_with('key')

    def test_get_crdt(self):
        obj = RiakMap(value={
            ('a', 'counter'): 1
        })

        bucket = MagicMock()
        bucket.get.return_value = obj
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        result = self.drv._get(self.drv.conn, 'key')

        self.assertIsInstance(result, Map)
        self.assertEqual(result.current, {'a_counter': 1})
        bucket.get.assert_called_with('key')

    def test_get_fail(self):
        obj = MagicMock()
        obj.__class__ = RiakObject
        obj.exists = False

        bucket = MagicMock()
        bucket.get.return_value = obj
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        with self.assertRaises(KeyError):
            self.drv._get(self.drv.conn, 'key')

        bucket.get.assert_called_with('key')

        obj = MagicMock()
        bucket.get.return_value = obj

        with self.assertRaises(KeyError):
            self.drv._get(self.drv.conn, 'key')

        bucket.get.assert_called_with('key')

    def test_multiget(self):
        obj0 = MagicMock()
        obj0.__class__ = RiakObject
        obj0.data = 'expected'

        obj1 = RiakMap(value={
            ('a', 'counter'): 1
        })

        bucket = MagicMock()
        bucket.multiget.return_value = [obj0, obj1]
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        result = self.drv._multiget(self.drv.conn, ['foo', 'bar'])

        bucket.multiget.assert_called_with(['foo', 'bar'])
        self.assertEqual(result[0], 'expected')
        self.assertEqual(result[1].current, {'a_counter': 1})

    def test_multiget_fail(self):
        err0 = (None, None, 'foo', Exception('not found'))

        bucket = MagicMock()
        bucket.multiget.return_value = [err0]
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        with self.assertRaises(KeyError):
            self.drv._multiget(self.drv.conn, ['foo', 'bar'])

        bucket.multiget.assert_called_with(['foo', 'bar'])

    def test_put(self):
        bucket = MagicMock()
        obj = MagicMock()
        bucket.new.return_value = obj
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        self.drv._put(self.drv.conn, 'key', {
            'foo': 'bar'
        })

        obj.store.assert_called_with()
        bucket.new.assert_called_with('key', {'foo': 'bar'})

    def test_put_crdt(self):
        bucket = MagicMock()
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        crdt = Map()
        crdt['a_counter'].increment()

        result = self.drv._put(self.drv.conn, 'key', crdt)

        self.assertIsNone(result)
        self.assertTrue(bucket._client.update_datatype.called)
        self.assertEqual(len(bucket._client.update_datatype.call_args), 2)
        self.assertEqual(len(bucket._client.update_datatype.call_args[0]), 1)
        self.assertIsInstance(
            bucket._client.update_datatype.call_args[0][0],
            RiakMap
        )
        self.assertEqual(
            bucket._client.update_datatype.call_args[1],
            {'return_body': True}
        )

    def test_multiput(self):
        bucket = MagicMock()

        self._instanciate()
        self.drv.connect()

        obj = MagicMock()
        crdt = Map()
        crdt['a_counter'].increment()

        bucket.new.return_value = obj
        self.conn.bucket.return_value = bucket

        self.drv._multiput(self.drv.conn, ['foo', 'bar'], [obj, crdt])
        self.conn.multiput.assert_called_with([obj])

    def test_remove(self):
        obj = MagicMock()
        obj.exists = True

        bucket = MagicMock()
        bucket.get.return_value = obj
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        self.drv._remove(self.drv.conn, 'key')

        bucket.get.assert_called_with('key')
        obj.delete.assert_called_with()

    def test_remove_fail(self):
        obj = MagicMock()
        obj.exists = False

        bucket = MagicMock()
        bucket.get.return_value = obj
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        with self.assertRaises(KeyError):
            self.drv._remove(self.drv.conn, 'key')

        bucket.get.assert_called_with('key')
        obj.delete.assert_not_called()

    def test_exists(self):
        obj = MagicMock()

        bucket = MagicMock()
        bucket.get.return_value = obj
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        obj.exists = True
        result = self.drv._exists(self.drv.conn, 'key')
        self.assertTrue(result)
        bucket.get.assert_called_with('key')

        obj.exists = False
        result = self.drv._exists(self.drv.conn, 'key')
        self.assertFalse(result)
        bucket.get.assert_called_with('key')

    def test_keys(self):
        bucket = MagicMock()
        bucket.get_keys.return_value = ['foo', 'bar']
        self.conn.bucket.return_value = bucket

        self._instanciate()
        self.drv.connect()

        result = self.drv._keys(self.drv.conn)
        self.assertEqual(result, ['foo', 'bar'])


if __name__ == '__main__':
    main()
