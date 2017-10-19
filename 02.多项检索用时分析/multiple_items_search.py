# -*- coding: utf-8 -*-

"""
多项检索用时分析
"""

import MySQLdb
from time import clock
from solrclouds import SolrCloud


MYSQL_HOST = "10.0.1.68"
MYSQL_USER = "root"
MYSQL_PASS = "root123"
MYSQL_DB = "quality"

SQL_PATTERN = """
    SELECT
        callNumber, areaOfJob
    FROM
        Voice_20150329
    LIMIT
        {key_number};
"""

KEY_NUMBER = 50  # 一共有多少录音（最大值是 100万）
GROUP_NUMBER = 50  # 录音分组，多少录音查一次
START_TIME = 1422720000000  # 查询开始时间
END_TIME = 1423497600000  # 查询结束时间


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
    sql = SQL_PATTERN.format(key_number=KEY_NUMBER)
    cursor.execute(sql)
    items = cursor.fetchall()
    return grouped_item_by(GROUP_NUMBER, items)


def search_by_solr(items):
    sc = SolrCloud()
    r = sc.search(items, START_TIME, END_TIME)
    print("search_by_solr")
    print(len(r))


def main():
    start = clock()
    items = get_items()
    print len(items)
    search_by_solr(items)
    finish = clock()
    print "F {:10.6} s".format(finish - start)


if __name__ == "__main__":
    main()
