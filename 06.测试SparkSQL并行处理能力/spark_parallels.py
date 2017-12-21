# -*- coding: utf-8 -*-

"""
测试 SparkSQL 并行查询处理能力
"""

import click
import threading
from decorators import time_analyze
import random
from pyhive import hive
import os
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# click 模块配置
CLICK_CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"], terminal_width=100)
MODES = click.Choice(["common", "thrift"])


def get_file_contents(filepath):
    r = []
    with open(filepath, "r") as f:
        r = f.readlines()

    return r


# 待查询的SQL集合
SQLS = get_file_contents("{0}/sqls.sql".format(BASE_DIR))


def _init_spark_env():
    """ 初始化 Spark 运行环境 """
    SPARK_APP_NAME = "spark_parallels"
    SPARK_MASTER = "spark://server349:7077"
    SPARK_HOME = "/opt/spark-2.0.2-bin-hadoop2.6"
    PYSPARK_DIR = os.path.normpath(SPARK_HOME + "/python")
    PY4J_DIR = os.path.normpath(SPARK_HOME + "/python/lib/py4j-0.10.3-src.zip")

    if "SPARK_HOME" not in os.environ:
        os.environ["SPARK_HOME"] = SPARK_HOME

    sys.path.insert(0, PYSPARK_DIR)
    sys.path.insert(0, PY4J_DIR)

    from pyspark import SparkContext, SparkConf
    from pyspark.sql import HiveContext

    SPARK_ENVS = {
        "spark.executor.memory": "20g",
        "spark.cores.max": 20,
    }

    SPARK_CONF = SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER)
    for k, v in SPARK_ENVS.items():
        SPARK_CONF.set(k, v)

    SPARK_CONTEXT = SparkContext(conf=SPARK_CONF)

    global SQL_CONTEXT
    SQL_CONTEXT = HiveContext(SPARK_CONTEXT)


def get_random_sql():
    return SQLS[random.randint(0, len(SQLS) - 1)]


@time_analyze
def spark_sql_common_mode_parallels():
    """ 正常提交 Spark 任务（Spark Submit） """
    sql = get_random_sql()
    print(sql)
    print(SQL_CONTEXT.sql(sql).count())


@time_analyze
def spark_sql_thrift_mode_parallels():
    """
    通过 Thrift 方式提交 SQL

    > cd /opt/spark-2.0.2-bin-hadoop2.6
    > sbin/start-thriftserver.sh --master spark://10.0.3.49:7077 \
                                 --executor-memory 20G \
                                 --total-executor-cores 20
    """
    cursor = hive.connect(host="10.0.3.49", port="10003").cursor()
    sql = get_random_sql()
    print(sql)
    cursor.execute(sql)
    print(len(cursor.fetchall()))


@time_analyze
def parallel_by_threads(parallels, func):
    threads = []
    for i in range(0, parallels):
        t = threading.Thread(target=func, name="spt")
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()


@click.command(context_settings=CLICK_CONTEXT_SETTINGS)
@click.option("--parallels", default=1, help="并行数量")
@click.option("--mode", type=MODES, default="common", help="查询方式")
def main(parallels, mode):
    mode = mode.upper()
    if mode == "COMMON":
        func = spark_sql_common_mode_parallels
        _init_spark_env()
    elif mode == "THRIFT":
        func = spark_sql_thrift_mode_parallels

    parallel_by_threads(parallels, func)


if __name__ == "__main__":
    main()
