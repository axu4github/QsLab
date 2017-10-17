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


def get_keys():
    """ 获取所有 '项' 内容 """
    db = MySQLdb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASS, MYSQL_DB)
    cursor = db.cursor()
    sql = SQL_PATTERN.format(
        key_number=KEY_NUMBER)
    cursor.execute(sql)
    return cursor.fetchall()


def main():
    # 打开数据库连接
    print get_keys()


if __name__ == "__main__":
    main()
