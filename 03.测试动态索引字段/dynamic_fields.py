# -*- coding: utf-8 -*-

"""
测试 Solr 动态索引字段
"""

from mysqls import Mysql
import csv

SQL = """
    SELECT
        id,batch,statisticId,isForward,voiceFileId,voiceFileName,
        voiceTime,associationColumns,areaOfJob,busiType,callType,
        callNumber,accountNo,busiNo,repNo,repGroup,statusInfo,mute,
        riskExponent,muteDuration,speed,interrupt,longTalk,scores,
        ruleResultModel,ruleResultList,ruleResult,startTime,endTime,active,
        checkStatus,repTime,isAssociation,isReal,associationActive,
        char_length(ruleResultModel) as ruleResultModelLength
    FROM
        QualityFile_20171030
    ORDER BY
        ruleResultModelLength DESC
    LIMIT 10;
"""

OUTPUT_FILE = "result.csv"


def main():
    items = Mysql().execute(SQL)
    with open(OUTPUT_FILE, "wb") as f:
        w = csv.DictWriter(f, items[0].keys())
        w.writeheader()
        for i, item in enumerate(items):
            print(i + 1)
            w.writerow(item)


if __name__ == "__main__":
    main()
