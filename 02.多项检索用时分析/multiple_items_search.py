# -*- coding: utf-8 -*-

"""
多项检索用时分析
"""

import MySQLdb
from solrcloudpy.connection import SolrConnection
from time import clock


MYSQL_HOST = "10.0.1.68"
MYSQL_USER = "root"
MYSQL_PASS = "root123"
MYSQL_DB = "quality"
KEY_NUMBER = 80000
SQL_PATTERN = """
    SELECT
        callNumber, areaOfJob
    FROM
        Voice_20150329
    LIMIT
        {key_number};
"""
GROUP_NUMBER = 100  # 每组中'项'数量

SOLR_NODES = ["10.0.1.27:8983", "10.0.1.28:8983"]
SOLR_VERSION = "5.5.1"
SOLR_COLLECTION = "collection1"


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
            query.append("(callnumber:'{call_number}' AND area_of_job:'{area_of_job}')".format(
                call_number=call_number, area_of_job=area_of_job))
            query_str = " OR ".join(query)

        querys.append(query_str)

    return querys


def search_by_solr(items):
    coll = SolrConnection(SOLR_NODES, version=SOLR_VERSION)[SOLR_COLLECTION]
    i = 1 
    for query in get_solr_querys(items):
        start = clock()
        coll.search({"q": query})
        finish = clock()
        print "{} {:10.6} s".format(i, finish - start)
        i += 1


def main():
    start = clock()
    items = get_items()
    print len(items)
    search_by_solr(items)
    finish = clock()
    print "F {:10.6} s".format(finish - start)


if __name__ == "__main__":
    main()
