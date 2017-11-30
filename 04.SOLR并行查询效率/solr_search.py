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

# click 模块配置
CLICK_CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"], terminal_width=100)
QUERY_GROUP_NUMBER = 100  # 每次拼多少个条件查询一次
THREAD_NUMBER = 20  # 多少个线程并行查询
SOLR_NODES = ["10.0.1.27:8983", "10.0.1.28:8983"]
SOLR_VERSION = "5.5.1"
SOLR_COLLECTION = "collection1"
SOLR_ROWS = 1000000
# SOLR_ROWS = 10000
SOLR_TIMEOUT = 6000

r_number = 0  # 查询总数


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

    fq_query = " OR ".join(query_arr)
    base_query = "(start_time: [{start} TO {end}])".format(
        start=start_time, end=end_time)
    # base_query = " OR ".join(query_arr)
    # fq_query = "start_time: [{start} TO {end}]".format(
    #     start=start_time, end=end_time)
    base_query = "{} AND ({})".format(base_query, fq_query)

    se = SearchOptions()
    # se.commonparams.q(base_query).fq(fq_query)
    se.commonparams.q(base_query)
    se.commonparams.fl("id").start(0).rows(SOLR_ROWS)
    # 执行查询
    r = coll.search(se)
    docs = r.result.response.docs

    global r_number
    r_number += len(docs)
    return docs


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
    """ 根据 文件名 获取 开始时间 和 结束时间 """
    file_name = os.path.basename(file_path)
    time_str = file_name.split("_")[1]
    time_delta = datetime.timedelta(days=1)
    start_datetime = datetime.datetime.strptime(time_str, "%Y%m%d")
    start_time = datetime2timestamp(start_datetime)
    end_time = datetime2timestamp(start_datetime + time_delta)
    return (start_time, end_time)


def parallel_by_threads(querys, start_time, end_time):
    """ 通过线程进行并行查询 """
    i = 1
    thread_groups = grouped_item_by(THREAD_NUMBER, querys)
    print("Loop Number: [{}], Pre Thread Number: [{}]".format(
        len(thread_groups), THREAD_NUMBER))
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
        print("Seach Total Number: [{}]".format(r_number))
        exit()
        i += 1

    pass


@click.command(context_settings=CLICK_CONTEXT_SETTINGS)
@click.option("--file_path", default=None, help="待查询的条件")
@click.option("--query_group_number",
              default=QUERY_GROUP_NUMBER, help="多少个条件拼一个查询（默认: 100）")
@click.option("--thread_number",
              default=THREAD_NUMBER, help="多少个线程并行查询（默认: 20）")
def main(file_path, query_group_number, thread_number):
    if file_path is not None:
        if query_group_number is not None:
            global QUERY_GROUP_NUMBER
            QUERY_GROUP_NUMBER = query_group_number

        if thread_number is not None:
            global THREAD_NUMBER
            THREAD_NUMBER = thread_number

        (start_time, end_time) = get_range_time(file_path)
        querys = get_querys_from_file(file_path)
        group_querys = grouped_item_by(QUERY_GROUP_NUMBER, querys)
        print("Total: [{}], Group: [{}], Pre Group Number: [{}]".format(
            len(querys), len(group_querys), QUERY_GROUP_NUMBER))

        parallel_by_threads(group_querys, start_time, end_time)

    pass


if __name__ == "__main__":
    main()
