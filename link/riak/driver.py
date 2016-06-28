# -*- coding: utf-8 -*-

from b3j0f.conf import Configurable, category, Parameter
from link.kvstore.driver import Driver

from link.riak.feature import RiakSearch2
from link.riak import CONF_BASE_PATH

from six import string_types, raise_from
import riak


@Configurable(
    paths='{0}/driver.conf'.format(CONF_BASE_PATH),
    conf=category(
        'RIAK',
        Parameter(name='default_bucket', value='default'),
        Parameter(name='indexing', ptype=bool, value=False),
        Parameter(name='protocol', value='http'),
        Parameter(name='pkey'),
        Parameter(name='cert'),
        Parameter(name='cacert'),
        Parameter(name='crl'),
        Parameter(name='ciphers'),
        Parameter(name='sslver', ptype=int)
    )
)
class RiakDriver(Driver):

    __features__ = [RiakSearch2]
    __protocols__ = ['riak']

    def __init__(
        self,
        default_bucket=None,
        indexing=False,
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
        self.indexing = indexing
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

            security = riak.security.SecurityCreds(**kwargs)

        else:
            security = None

        return riak.RiakClient(nodes=nodes, credentials=security)

    def _disconnect(self, conn):
        conn.close()

    def _isconnected(self, conn):
        return conn is not None and conn.is_alive()

    def _get_bucket(self, conn):
        if len(self.path) == 0:
            bucket = conn.bucket(self.default_bucket)

        elif len(self.path) == 1:
            bucket = conn.bucket(self.path[0])

        else:
            bucket_type = conn.bucket_type(self.path[0])
            bucket = bucket_type.bucket('_'.join(self.path[1:]))

        return bucket

    def _get_indexes(self, val, prefix=''):
        pairs = []

        if isinstance(val, int):
            pairs.append(('{0}_int'.format(prefix), val))

        elif isinstance(val, string_types):
            pairs.append(('{0}_bin'.format(prefix), val))

        elif isinstance(val, dict):
            for key in val:
                pairs += self._get_indexes(
                    val[key],
                    '{0}_{1}'.format(prefix, key)
                )

        elif isinstance(val, list):
            for i in range(len(val)):
                item = val[i]

                pairs += self._get_indexes(
                    item,
                    '{0}_{1}'.format(prefix, i)
                )

        return pairs

    def _new_object(self, conn, key, val):
        obj = self._get_bucket(conn).new(key, val)

        if self.indexing:
            for indexkey, indexval in self._get_indexes(val):
                obj.add_index(indexkey, indexval)

        return obj

    def _get(self, conn, key):
        obj = self._get_bucket(conn).get(key)

        if not obj.exists:
            raise KeyError('No such key: {0}'.format(key))

        return obj.data

    def _multiget(self, conn, keys):
        bucket = self._get_bucket(conn)

        results = bucket.multiget(keys)

        datas = []

        for result in results:
            if isinstance(result, riak.RiakObject):
                datas.append(result.data)

            elif isinstance(result, riak.datatypes.Datatype):
                doc = {}

                for subkey, type_subkey in result.value:
                    dockey = '{0}_{1}'.format(subkey, type_subkey)
                    val = result.value[(subkey, type_subkey)]

                    if isinstance(val, frozenset):
                        val = list(val)

                    else:
                        val = str(val)

                    doc[dockey] = val

                datas.append(doc)

            else:
                _, _, key, err = result

                raise_from(
                    KeyError('No such key: {0}'.format(key)),
                    err
                )

        return datas

    def _put(self, conn, key, val):
        obj = self._new_object(conn, key, val)
        obj.store()

    def _multiput(self, conn, keys, vals):
        objs = [
            self._new_object(conn, k, v)
            for k, v in zip(keys, vals)
        ]
        conn.multiput(objs)

    def _remove(self, conn, key):
        obj = self._get_bucket(conn).get(key)

        if not obj.exists:
            raise KeyError('No such key: {0}'.format(key))

        if self.indexing:
            for indexkey, indexval in self._get_indexes(obj.data):
                obj.remove_index(indexkey, indexval)

        obj.delete()

    def _exists(self, conn, key):
        obj = self._get_bucket(conn).get(key)
        return obj.exists

    def _keys(self, conn):
        return self._get_bucket(conn).get_keys()
