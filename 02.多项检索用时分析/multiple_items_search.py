# -*- coding: utf-8 -*-

"""
多项检索用时分析
"""

import click
import MySQLdb
from time import time
from solrclouds import SolrCloud
from hbases import Hbase
from functools import wraps
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


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

KEY_NUMBER = 10000  # 一共有多少录音（最大值是 100万）
GROUP_NUMBER = 100  # 录音分组，多少录音查一次
START_TIME = 1422720000000  # 查询开始时间
END_TIME = 1422723600000  # 查询结束时间


def time_analyze(func):
    """ 装饰器 获取程序执行时间 """
    @wraps(func)
    def consume(*args, **kwargs):
        # 重复执行次数（单次执行速度太快）
        exec_times = 1
        start = time()
        for i in range(exec_times):
            r = func(*args, **kwargs)

        finish = time()
        print("{:<20}{:10.6} s".format(func.__name__ + ":", finish - start))
        return r

    return consume


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


@time_analyze
def get_items():
    """ 获取所有 '项' 内容 """
    db = MySQLdb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASS, MYSQL_DB)
    cursor = db.cursor()
    sql = SQL_PATTERN.format(key_number=KEY_NUMBER)
    cursor.execute(sql)
    items = cursor.fetchall()
    return grouped_item_by(GROUP_NUMBER, items)


@time_analyze
def search_by_solr(items):
    sc = SolrCloud()
    r = sc.search(items, START_TIME, END_TIME)
    print("search_by_solr count: {}".format(str(len(r))))
    return r


@time_analyze
def get_metas(items):
    h = Hbase()
    metas = h.fetchs(items)
    print("get_metas count: {}".format(str(len(metas))))
    return metas


@time_analyze
def get_solr_results_from_file(file_path):
    """ 从文件中获取 Solr 结果 """
    items = []
    with open(file_path) as f:
        for l in f.readlines():
            items.append(l.rstrip("\n"))

    return items


@click.command()
@click.option("--file_path", default=None, help="SOLR结果文件")
def main(file_path):
    start = time()
    if file_path is not None:
        base_items = get_solr_results_from_file(file_path)
    else:
        base_items = search_by_solr(get_items())

    print("Base Items Number => [{}].".format(len(base_items)))
    get_metas(base_items)
    finish = time()
    print("Total Run Time => [{} s].".format(finish - start))


if __name__ == "__main__":
    main()
