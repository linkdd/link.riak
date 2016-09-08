# -*- coding: utf-8 -*-

from b3j0f.conf import Configurable, category, Parameter

from link.middleware.core import register_middleware
from link.kvstore.driver import Driver

from link.crdt.core import CRDT

from link.riak.features.crdt import convert_crdt_from_riak
from link.riak.features.crdt import convert_crdt_to_riak
from link.riak.features.fulltext import RiakSearch2
from link.riak import CONF_BASE_PATH
from link.feature import addfeatures

from riak.security import SecurityCreds
from riak import RiakClient, RiakObject
from riak.datatypes import Datatype

from six import raise_from


@Configurable(
    paths='{0}/driver.conf'.format(CONF_BASE_PATH),
    conf=category(
        'RIAK',
        Parameter(name='default_bucket', value='default'),
        Parameter(name='protocol', value='http'),
        Parameter(name='pkey'),
        Parameter(name='cert'),
        Parameter(name='cacert'),
        Parameter(name='crl'),
        Parameter(name='ciphers'),
        Parameter(name='sslver', ptype=int)
    )
)
@addfeatures([RiakSearch2])
@register_middleware
class RiakDriver(Driver):

    __protocols__ = ['riak']

    def __init__(
        self,
        default_bucket=None,
        protocol=None,
        pkey=None,
        cert=None,
        cacert=None,
        crl=None,
        ciphers=None,
        sslver=5,
        *args, **kwargs
    ):
        super(RiakDriver, self).__init__(*args, **kwargs)

        self.default_bucket = default_bucket
        self.protocol = protocol
        self.pkey = pkey
        self.cert = cert
        self.cacert = cacert
        self.crl = crl
        self.ciphers = ciphers
        self.sslver = sslver

    def _connect(self):
        nodes = []
        security = None

        for host, port in self.hosts:
            node = {
                'host': host
            }

            if self.protocol == 'pbc':
                node['pb_port'] = port

            else:
                node['http_port'] = port

            nodes.append(node)

        kwargs = {}

        if self.user is not None:
            kwargs['username'] = self.user

            if self.pwd:
                kwargs['password'] = self.pwd

        if self.pkey is not None:
            kwargs['pkey_file'] = self.pkey

        if self.cert is not None:
            kwargs['cert_file'] = self.cert

        if self.cacert is not None:
            kwargs['cacert_file'] = self.cacert

        if self.crl is not None:
            kwargs['crl_file'] = self.crl

        if self.ciphers is not None:
            kwargs['ciphers'] = self.ciphers

        if kwargs:
            kwargs['ssl_version'] = self.sslver

            security = SecurityCreds(**kwargs)

        else:
            security = None

        return RiakClient(nodes=nodes, credentials=security)

    def _disconnect(self, conn):
        conn.close()

    def _isconnected(self, conn):
        return conn is not None and conn.is_alive()

    def _get_bucket(self, conn):
        if self.path is None or len(self.path) == 0:
            bucket = conn.bucket(self.default_bucket)

        elif len(self.path) == 1:
            bucket = conn.bucket(str(self.path[0]))

        else:
            bucket_type = conn.bucket_type(str(self.path[0]))
            bucket = bucket_type.bucket('_'.join([
                str(p)
                for p in self.path[1:]
            ]))

        return bucket

    def _new_object(self, conn, key, val):
        if not isinstance(val, CRDT):
            obj = self._get_bucket(conn).new(key, val)
            return obj

        else:
            crdt = val

        riak_crdt = convert_crdt_to_riak(crdt)
        riak_crdt.bucket = self._get_bucket(conn)
        riak_crdt.key = key
        riak_crdt.update()

    def _get(self, conn, key):
        key = str(key)

        obj = self._get_bucket(conn).get(key)

        if isinstance(obj, RiakObject):
            if not obj.exists:
                raise KeyError('No such key: {0}'.format(key))

            return obj.data

        elif isinstance(obj, Datatype):
            return convert_crdt_from_riak(obj)

        else:
            raise KeyError('No such key: {0}'.format(key))

    def _multiget(self, conn, keys):
        keys = list(map(str, keys))
        bucket = self._get_bucket(conn)

        results = bucket.multiget(keys)

        datas = []

        for result in results:
            if isinstance(result, RiakObject):
                datas.append(result.data)

            elif isinstance(result, Datatype):
                doc = convert_crdt_from_riak(result)
                datas.append(doc)

            else:
                _, _, key, err = result

                raise_from(
                    KeyError('No such key: {0}'.format(key)),
                    err
                )

        return datas

    def _put(self, conn, key, val):
        key = str(key)
        obj = self._new_object(conn, key, val)

        if obj is not None:
            obj.store()

    def _multiput(self, conn, keys, vals):
        keys = list(map(str, keys))
        objs = list(filter(
            lambda obj: obj is not None,
            [
                self._new_object(conn, k, v)
                for k, v in zip(keys, vals)
            ]
        ))

        conn.multiput(objs)

    def _remove(self, conn, key):
        key = str(key)
        obj = self._get_bucket(conn).get(key)

        if not obj.exists:
            raise KeyError('No such key: {0}'.format(key))

        obj.delete()

    def _exists(self, conn, key):
        key = str(key)
        obj = self._get_bucket(conn).get(key)
        return obj.exists

    def _keys(self, conn):
        return self._get_bucket(conn).get_keys()
