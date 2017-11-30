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
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


GROUP_NUMBER = 20
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


# @time_analyze
def solr_search(query, start_time, end_time):
    conn = SolrConnection(
        SOLR_NODES, version=SOLR_VERSION, timeout=SOLR_TIMEOUT)
    coll = conn[SOLR_COLLECTION]
    # 查询条件准备
    base_query = "(area_of_job:{aof} AND callnumber:{cn})".format(
        aof=query["aof"], cn=query["cn"])
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

    print(len(querys))
    return grouped_item_by(GROUP_NUMBER, querys)


def datetime2timestamp(datetime):
    return int(time.mktime(datetime.timetuple())) * 1000


@click.command()
@click.option("--file_path", default=None, help="待查询的条件")
def main(file_path):
    group_querys = get_querys_from_file(file_path)
    print(len(group_querys))
    time_str = file_path.split("_")[1]
    time_delta = datetime.timedelta(days=1)
    start_datetime = datetime.datetime.strptime(time_str, "%Y%m%d")
    start_time = datetime2timestamp(start_datetime)
    end_time = datetime2timestamp(start_datetime + time_delta)

    i = 1
    for group_query in group_querys:
        s = time.time()
        threads = []
        for query in group_query:
            t = threading.Thread(target=solr_search, name="search_threads",
                                 args=(query, start_time, end_time,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        e = time.time()
        print("group-{}, {}s".format(i, e - s))
        # exit()
        i += 1


if __name__ == "__main__":
    main()
