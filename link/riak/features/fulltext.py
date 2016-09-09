# -*- coding: utf-8 -*-

from link.fulltext.feature import FulltextIndex


class RiakSearch2(FulltextIndex):

    DATA_ID = '_yz_rk'

    def search(self, query):
        result = self.obj.conn.fulltext_search(
            str(self.obj.path[0]),
            query
        )

        return result['docs']
