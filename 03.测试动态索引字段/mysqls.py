# -*- coding: utf-8 -*-

import MySQLdb
from collections import OrderedDict

MYSQL_HOST = "10.0.1.68"
MYSQL_USER = "root"
MYSQL_PASS = "root123"
MYSQL_DB = "quality"

# 26个大写字母
ABCD = [chr(i).lower() for i in range(97, 123)]
RULE_PATTERN = "rule_{index}"


class Mysql(object):

    def __init__(self, *args, **kwargs):
        super(Mysql, self).__init__(*args, **kwargs)
        self.db = MySQLdb.connect(
            MYSQL_HOST, MYSQL_USER, MYSQL_PASS, MYSQL_DB)
        self.cursor = self.db.cursor()

    def execute(self, sql):
        """ 执行查询语句 """
        self.cursor.execute(sql)
        items = self.cursor.fetchall()
        field_names = [desc[0] for desc in self.cursor.description]
        return self.format_result(items, field_names)

    def format_result(self, items, field_names):
        """ 格式化查询结果 """
        result = []
        if items is not None:
            full_field_names = self.get_full_field_names(items[0], field_names)
            for item in items:
                tmp_result = OrderedDict()
                rule_index = 0
                for i, field_name in enumerate(full_field_names):
                    # 设置除了规则以外的值
                    try:
                        tmp_result[field_name] = item[i]
                    # 设置规则值
                    except Exception:
                        rules = item[24]
                        rule_field_name = RULE_PATTERN.format(
                            index=ABCD[rule_index])
                        tmp_result[rule_field_name] = rules[rule_index]
                        rule_index += 1

                result.append(tmp_result)

        return result

    def get_full_field_names(self, item, field_names):
        """ 获取所有字段名称（主要完成基于规则字段的拆分） """
        rules = item[24]
        for i, rule in enumerate(rules):
            field_names.append(RULE_PATTERN.format(index=ABCD[i]))

        return field_names
