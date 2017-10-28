# -*- coding: utf-8 -*-

import happybase
from time import time

HBASE_HOST = "10.0.3.41"
HBASE_TABLE = "smartv"
GET_ROW_NUMBER = 10000
GROUP_NUMBER = 10000


class Hbase(object):
    """ Hbase 客户端 """

    def __init__(self, *args, **kwargs):
        super(Hbase, self).__init__(*args, **kwargs)
        self.hbase_client = happybase.Connection(HBASE_HOST, timeout=600000)
        self.table = self.hbase_client.table(HBASE_TABLE)

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

    def fetchs(self, row_keys):
        i = 1
        result = []
        groups = self.grouped_item_by(GROUP_NUMBER, row_keys)
        for group in groups:
            s = time()
            result.append(self.table.rows(group))
            e = time()
            print("{}, {}, {}".format(i, len(group), e - s))
            i += 1

        return result
