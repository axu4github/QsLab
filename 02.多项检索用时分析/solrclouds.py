# -*- coding: utf-8 -*-

from solrcloudpy.connection import SolrConnection
from solrcloudpy.parameters import SearchOptions
from time import clock


SOLR_NODES = ["10.0.1.27:8983", "10.0.1.28:8983"]
SOLR_VERSION = "5.5.1"
SOLR_COLLECTION = "collection1"
SOLR_ROWS = 1000


class SolrCloud(object):
    """ SolrCloud 查询 """

    def __init__(self, *args, **kwargs):
        super(SolrCloud, self).__init__(*args, **kwargs)
        self.conn = SolrConnection(SOLR_NODES, version=SOLR_VERSION)
        self.coll = self.conn[SOLR_COLLECTION]

    def get_solr_querys(self, items, start_time, end_time):
        querys = []
        for item_group in items:
            query = []
            for item in item_group:
                (call_number, area_of_job) = item
                query.append("(callnumber:*{call_number} AND area_of_job:{area_of_job})".format(
                    call_number=call_number, area_of_job=area_of_job))
                query_str = " OR ".join(query)

            # 每一次查询添加一个时间范围
            query_str = "start_time:[{start_time} TO {end_time}] AND {query_str}".format(
                start_time=start_time, end_time=end_time, query_str=query_str)

            # 将每次查询条件添加到查询条件数组中
            querys.append(query_str)

        return querys

    def perform_search(self, query, start, rows):
        """ 执行检索 """
        se = SearchOptions()
        se.commonparams.q(query).fl("id").start(start).rows(rows)
        solr_response = self.coll.search(se)
        return solr_response.result

    def search(self, items, start_time, end_time):
        """ 检索 """
        i = 0
        results = []
        for query in self.get_solr_querys(items, start_time, end_time):
            start_rows = 0
            start = clock()
            r = self.perform_search(query, start_rows, SOLR_ROWS).response
            r_num = r.numFound
            if r_num > 0:
                results += [doc["id"] for doc in r.docs]
                if r_num > SOLR_ROWS:
                    while True:
                        print(start_rows)
                        if start_rows > r_num:
                            break

                        start_rows += SOLR_ROWS
                        tmp_r = self.perform_search(
                            query, start_rows, SOLR_ROWS).response
                        results += [doc["id"] for doc in tmp_r.docs]
                        print(len(results))

            finish = clock()
            print("{} {:10.6} s".format(i, finish - start))
            i += 1
            exit()
