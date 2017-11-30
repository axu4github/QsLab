# -*- coding: utf-8 -*-

"""
测试SOLR并行测试
"""

import click
import threading
import datetime
import time
from functools import wraps
from solrcloudpy.connection import SolrConnection
from solrcloudpy.parameters import SearchOptions
import os
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

QUERY_GROUP_NUMBER = 100  # 每次拼多少个条件查询一次
THREAD_GROUP_NUMBER = 20  # 多少个线程并行查询
SOLR_NODES = ["10.0.1.27:8983", "10.0.1.28:8983"]
SOLR_VERSION = "5.5.1"
SOLR_COLLECTION = "collection1"
SOLR_ROWS = 1000000
SOLR_TIMEOUT = 6000


def time_analyze(func):
    """ 装饰器 获取程序执行时间 """
    @wraps(func)
    def consume(*args, **kwargs):
        # 重复执行次数（单次执行速度太快）
        exec_times = 1
        start = time.time()
        for i in range(exec_times):
            r = func(*args, **kwargs)

        finish = time.time()
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
def solr_search(querys, start_time, end_time):
    conn = SolrConnection(
        SOLR_NODES, version=SOLR_VERSION, timeout=SOLR_TIMEOUT)
    coll = conn[SOLR_COLLECTION]
    # 查询条件准备
    query_arr = []
    for query in querys:
        query_str = "(area_of_job:{aof} AND callnumber:{cn})".format(
            aof=query["aof"], cn=query["cn"])
        query_arr.append(query_str)

    base_query = " OR ".join(query_arr)
    fq_query = "start_time: [{start} TO {end}]".format(
        start=start_time, end=end_time)
    se = SearchOptions()
    se.commonparams.q(base_query).fq(fq_query)
    se.commonparams.fl("id").start(0).rows(SOLR_ROWS)
    # 执行查询
    response = coll.search(se)
    return response.result


@time_analyze
def get_querys_from_file(file_path):
    """ 从文件中获取查询条件 """
    querys = []
    with open(file_path) as f:
        for l in f.readlines():
            tmp = l.rstrip("\n").split(",")
            querys.append({"aof": tmp[0], "cn": tmp[1]})

    return querys


def datetime2timestamp(datetime):
    return int(time.mktime(datetime.timetuple())) * 1000


def get_range_time(file_path):
    file_name = os.path.basename(file_path)
    time_str = file_name.split("_")[1]
    time_delta = datetime.timedelta(days=1)
    start_datetime = datetime.datetime.strptime(time_str, "%Y%m%d")
    start_time = datetime2timestamp(start_datetime)
    end_time = datetime2timestamp(start_datetime + time_delta)
    return (start_time, end_time)


@click.command()
@click.option("--file_path", default=None, help="待查询的条件")
def main(file_path):
    (start_time, end_time) = get_range_time(file_path)
    querys = get_querys_from_file(file_path)
    group_querys = grouped_item_by(QUERY_GROUP_NUMBER, querys)
    print("Total: [{}], Group: [{}], Pre Group Number: [{}]".format(
        len(querys), len(group_querys), QUERY_GROUP_NUMBER))

    i = 1
    thread_groups = grouped_item_by(THREAD_GROUP_NUMBER, group_querys)
    print("Loop Number: [{}], Pre Thread Number: [{}]".format(
        len(thread_groups), THREAD_GROUP_NUMBER))
    for thread_group in thread_groups:
        s = time.time()
        threads = []
        for query in thread_group:
            t = threading.Thread(target=solr_search, name="search_threads",
                                 args=(query, start_time, end_time,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        e = time.time()
        print("Loop-{}, {}s".format(i, e - s))
        exit()
        i += 1


if __name__ == "__main__":
    main()
