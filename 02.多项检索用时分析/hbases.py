# -*- coding: utf-8 -*-

import happybase
import threading
from time import time

HBASE_HOST = "10.0.3.41"
HBASE_TABLE = "smartv"
GROUP_NUMBER = 10000
THREAD_GROUP_NUMBER = 10

r = []


class Hbase(object):
    """ Hbase 客户端 """

    def __init__(self, *args, **kwargs):
        super(Hbase, self).__init__(*args, **kwargs)
        # self.hbase_client = happybase.Connection(HBASE_HOST, timeout=600000)
        # self.table = self.hbase_client.table(HBASE_TABLE)

    def grouped_item_by(self, group_number, items):
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

    def fetch_group_by_thread(self, item_group):
        hbase_client = happybase.Connection(HBASE_HOST, timeout=600000)
        table = hbase_client.table(HBASE_TABLE)
        result_items = table.rows(item_group)
        r.extend(result_items)

    def fetchs(self, row_keys):
        i = 1
        groups = self.grouped_item_by(GROUP_NUMBER, row_keys)
        for group in groups:
            s = time()
            threads = []
            thread_groups = self.grouped_item_by(THREAD_GROUP_NUMBER, group)
            for thread_group in thread_groups:
                t = threading.Thread(
                    target=self.fetch_group_by_thread, name="t", args=(thread_group,))
                t.start()
                threads.append(t)

            # 等待所有线程跑完
            for t in threads:
                t.join()

            e = time()
            print("{}, {}, {}".format(i, len(r), e - s))
            i += 1

        return r
