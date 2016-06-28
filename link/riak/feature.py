# -*- coding: utf-8 -*-

from link.fulltext.feature import FulltextIndex


class RiakSearch2(FulltextIndex):
    def search(self, query):
        result = self.middleware.conn.fulltext_search(
            self.middleware.path[0],
            query
        )

        return result['docs']
