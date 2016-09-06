# -*- coding: utf-8 -*-

from b3j0f.utils.ut import UTCase
from mock import MagicMock
from unittest import main

from link.riak.features.fulltext import RiakSearch2


class TestFulltext(UTCase):
    def test_search(self):
        middleware = MagicMock()
        middleware.path = ['test']
        middleware.conn.fulltext_search.return_value = {
            'docs': 'expected'
        }

        rs2 = RiakSearch2(middleware)
        result = rs2.search('query')

        self.assertEqual(result, 'expected')
        middleware.conn.fulltext_search.assert_called_with('test', 'query')


if __name__ == '__main__':
    main()
