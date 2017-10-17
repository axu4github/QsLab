# -*- coding: utf-8 -*-

"""
多项检索用时分析
"""

import MySQLdb


MYSQL_HOST = "10.0.1.68"
MYSQL_USER = "root"
MYSQL_PASS = "root123"
MYSQL_DB = "quality"
KEY_NUMBER = 20
SQL_PATTERN = """
    SELECT
        callNumber, areaOfJob
    FROM
        Voice_20150329
    LIMIT
        {key_number};
"""
GROUP_NUMBER = 3  # 每组中'项'数量


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


def main():
    # 打开数据库连接
    print get_items()


if __name__ == "__main__":
    main()
