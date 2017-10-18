# -*- coding: utf-8 -*-

"""
多项检索用时分析
"""

import MySQLdb
from solrcloudpy.connection import SolrConnection
from solrcloudpy.parameters import SearchOptions
from time import clock


MYSQL_HOST = "10.0.1.68"
MYSQL_USER = "root"
MYSQL_PASS = "root123"
MYSQL_DB = "quality"
KEY_NUMBER = 100  # 一共有多少录音（最大值是 100万）
GROUP_NUMBER = 50  # 录音分组，多少录音查一次
START_TIME = 1422720000000  # 查询开始时间
END_TIME = 1423497600000  # 查询结束时间

SQL_PATTERN = """
    SELECT
        callNumber, areaOfJob
    FROM
        Voice_20150329
    LIMIT
        {key_number};
"""
SOLR_NODES = ["10.0.1.27:8983", "10.0.1.28:8983"]
SOLR_VERSION = "5.5.1"
SOLR_COLLECTION = "collection1"
SOLR_ROWS = 1000


def grouped_item_by(group_number, items):
    """ '项' 分组 """
    result = []
    loop_times = 0
    if group_number != 0:
        loop_times = (len(items) / group_number)
        if len(items) % group_number > 1:
            loop_times += 1

    start = 0
    for i in range(loop_times):
        end = start + group_number
        result.append(items[start:end])
        start = end

    return result


def get_items():
    """ 获取所有 '项' 内容 """
    db = MySQLdb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASS, MYSQL_DB)
    cursor = db.cursor()
    sql = SQL_PATTERN.format(
        key_number=KEY_NUMBER)
    cursor.execute(sql)
    items = cursor.fetchall()
    return grouped_item_by(GROUP_NUMBER, items)


def get_solr_querys(items):
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
            start_time=START_TIME, end_time=END_TIME, query_str=query_str)

        # 将每次查询条件添加到查询条件数组中
        querys.append(query_str)

    return querys


def search_by_solr(items):
    coll = SolrConnection(SOLR_NODES, version=SOLR_VERSION)[SOLR_COLLECTION]
    i = 1
    for query in get_solr_querys(items):
        start = clock()
        se = SearchOptions()
        se.commonparams.q(query).fl("id").rows(SOLR_ROWS)
        solr_response = coll.search(se)
        print(solr_response.result.response.numFound)
        finish = clock()
        print "{} {:10.6} s".format(i, finish - start)
        i += 1
        exit()


def main():
    start = clock()
    items = get_items()
    print len(items)
    search_by_solr(items)
    finish = clock()
    print "F {:10.6} s".format(finish - start)


if __name__ == "__main__":
    main()
