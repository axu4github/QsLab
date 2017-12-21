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

# click 模块配置
CLICK_CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"], terminal_width=100)

SQLS = [
    "select word, count(word) num from (select concat(w.wordsa,' ',w.wordsb) words from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000'  ) p left join (select filename, wordsa, wordsb from wordsfile_no_cache where starttime>='1501516800000' and starttime<='1504195199000') w on w.filename=p.filename left join (select file_name from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.file_name where s.file_name!='null' ) LATERAL VIEW explode(split(words, ' ')) as word group by word order by num desc",
    "select s.account_no, s.rep_no, s.call_number, p.filename, from_unixtime((s.start_time/1000),'yyyy-MM-dd HH:mm:ss') dateStr, v.typeofservice, concat(round(p.rate*100,3),'%') rate from (select filename,rate from predict_result where starttime>='1501516800000' and starttime<='1504195199000'  ) p inner join (select file_name filename, account_no, call_number, start_time, rep_no  from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.filename inner join (select filename , typeofservice from voicefile_no_cache where starttime>='1501516800000' and starttime<='1504195199000') v on v.filename=s.filename order by s.start_time desc",
    "select service timeStr , count(service) num from (select v.typeofservice services from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000'  ) p left join (select file_name from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.file_name left join (select filename, typeofservice from voicefile_no_cache where starttime>='1501516800000' and starttime<='1504195199000' ) v on v.filename=p.filename where s.file_name!='null' ) LATERAL VIEW explode(split(services, ' ')) as service where service!='' group by service order by num desc",
    "select p.date timeStr, count(p.date) num from (select filename, from_unixtime((starttime/1000),'yyyy-MM-dd') date from predict_result where starttime>='1501516800000' and starttime<='1504195199000'  ) p left join (select file_name from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000'  and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.file_name where s.file_name!='null' group by p.date order by p.date",
    "select s.rep_no tag , count(s.rep_no) num from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000'  ) p left join (select file_name, rep_no from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on s.file_name=p.filename where s.file_name!='null' group by s.rep_no order by num desc",
    "select s.rep_group timeStr, count(s.rep_group) num from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000'  ) p left join (select file_name, rep_group from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2' ) s on s.file_name=p.filename where s.file_name!='null' group by s.rep_group order by num desc",
    "select tag, count(tag) num from (select class, concat(split(class, '-')[0],'-',split(class, '-')[1]) tag from voicefile_no_cache LATERAL VIEW explode(split(classfication, ' ')) as class  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  ) group by tag order by num desc",
    "select count(1) num from voicefile_no_cache  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' ) ",
    "select a.word, a.num, b.voicenum from (select word, count(word) num from (select concat(w.wordsa,' ',w.wordsb) words from (select filename from voicefile_no_cache  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  ) v left join wordsfile_no_cache w on w.filename=v.filename ) r LATERAL VIEW explode(split(r.words, ' ')) as word group by word) a left join (select f.word, count(f.word) voicenum from (select word, count(r1.filename) from (select w.filename, concat(w.wordsa,' ',w.wordsb) words from (select filename from voicefile_no_cache  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  ) v left join wordsfile_no_cache w on w.filename=v.filename ) r1 LATERAL VIEW explode(split(r1.words, ' ')) as word group by word,r1.filename) f group by f.word ) b on a.word=b.word order by num desc",
    "select tag, count(tag) num from voicefile_no_cache LATERAL VIEW explode(split(classfication, ' ')) as tag  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  group by tag order by num desc",
    "select hour timeStr, count(hour) num from (select distinct(filename), from_unixtime((starttime/1000),'HH') hour from voicefile_no_cache LATERAL VIEW explode(split(classfication, ' ')) as tag  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  group by filename,starttime) group by hour order by hour",
    "select date timeStr, count(date) num from (select distinct(filename), from_unixtime((starttime/1000),'yyyy-MM-dd') date from voicefile_no_cache LATERAL VIEW explode(split(classfication, ' ')) as tag  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  group by filename,starttime) group by date order by date",
    "select tag, count(tag) num from voicefile_no_cache LATERAL VIEW explode(split(area, ' ')) as tag  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  group by tag order by num desc",
    "select tag, count(tag) num from voicefile_no_cache LATERAL VIEW explode(split(typeofservice, ' ')) as tag where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( tag = '发卡' or tag = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  group by tag order by num desc",
    "select hour timeStr, count(hour) num from (select distinct(filename), from_unixtime((starttime/1000),'HH') hour from voicefile_no_cache LATERAL VIEW explode(split(classfication, ' ')) as tag  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  and (tag = '11-6-149' or tag = '11-6-147' or tag = '11-6-142' or tag = '11-6-146' or tag = '11-6-145' or tag = '11-6-143' )  group by filename,starttime) group by hour order by hour",
    "select date timeStr, count(date) num from (select distinct(filename), from_unixtime((starttime/1000),'yyyy-MM-dd') date from voicefile_no_cache LATERAL VIEW explode(split(classfication, ' ')) as tag  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  and (tag = '11-6-149' or tag = '11-6-147' or tag = '11-6-142' or tag = '11-6-146' or tag = '11-6-145' or tag = '11-6-143' )  group by filename,starttime) group by date order by date",
    "select tag, count(tag) num from voicefile_no_cache LATERAL VIEW explode(split(area, ' ')) as tag  LATERAL VIEW explode(split(classfication, ' ')) as clazz  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  and (clazz = '11-6-149' or clazz = '11-6-147' or clazz = '11-6-142' or clazz = '11-6-146' or clazz = '11-6-145' or clazz = '11-6-143' )  group by tag order by num desc",
    "select tag, count(tag) num from voicefile_no_cache LATERAL VIEW explode(split(typeofservice, ' ')) as tag  LATERAL VIEW explode(split(classfication, ' ')) as clazz where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( tag = '发卡' or tag = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  and (clazz = '11-6-149' or clazz = '11-6-147' or clazz = '11-6-142' or clazz = '11-6-146' or clazz = '11-6-145' or clazz = '11-6-143' )  group by tag order by num desc",
    "select tag, count(tag) num from voicefile_no_cache LATERAL VIEW explode(split(classfication, ' ')) as tag  LATERAL VIEW explode(split(classfication, ' ')) as clazz  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  and (clazz = '11-6-149' or clazz = '11-6-147' or clazz = '11-6-142' or clazz = '11-6-146' or clazz = '11-6-145' or clazz = '11-6-143' )  group by tag order by num desc",
    "select a.word, a.num, b.voicenum from (select word, count(word) num from (select concat(w.wordsa,' ',w.wordsb) words from (select filename from voicefile_no_cache  LATERAL VIEW explode(split(classfication, ' ')) as tag  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  and (tag = '11-6-149' or tag = '11-6-147' or tag = '11-6-142' or tag = '11-6-146' or tag = '11-6-145' or tag = '11-6-143' )  ) v left join wordsfile_no_cache w on w.filename=v.filename ) r LATERAL VIEW explode(split(r.words, ' ')) as word group by word) a left join (select f.word, count(f.word) voicenum from (select word, count(r1.filename) from (select w.filename, concat(w.wordsa,' ',w.wordsb) words from (select filename from voicefile_no_cache  LATERAL VIEW explode(split(classfication, ' ')) as tag  LATERAL VIEW explode(split(typeofservice, ' ')) as typeofserviceitem where 1=1  and starttime>='1501516800000' and starttime<='1504195199000' and ( typeofserviceitem = '发卡' or typeofserviceitem = '转账' ) and (area = '北京' or area = '上海' or area = '广州' or area = '深圳' or area = '青岛' )  and (tag = '11-6-149' or tag = '11-6-147' or tag = '11-6-142' or tag = '11-6-146' or tag = '11-6-145' or tag = '11-6-143' )  ) v left join wordsfile_no_cache w on w.filename=v.filename ) r1 LATERAL VIEW explode(split(r1.words, ' ')) as word group by word,r1.filename) f group by f.word ) b on a.word=b.word order by num desc",
    "select sum(if(duration > '600', 1, 0)) SUM, count(1) TOTAL from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' ",
    "select count(t1.filename) COUNT from (select distinct(filename) filename from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600' and col1='投诉' ) t1",
    "select in2 DIS, count(distinct(filename)) as COUNT from voicefile_no_cache LATERAL VIEW explode(split(product, ' ')) mytab2 as in2 where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  and length(in2) > 0  group by in2",
    "select in1 DIS, count(distinct(filename)) as COUNT from voicefile_no_cache LATERAL VIEW explode(split(typeofservice, ' ')) mytab1 as in1 where length(in1) > 0 and starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  group by in1",
    "select in1 DIS, count(distinct(filename)) as COUNT, AVG(duration) duration from voicefile_no_cache LATERAL VIEW explode(split(classfication, ' ')) mytab1 as in1 where length(in1) > 0 and starttime > '1501516800000' and starttime < '1504195200000'  and duration > '600'  group by in1",
    "select from_unixtime(CAST(substring(starttime, 0, 10) as int), 'yyyy-MM-dd') DIS, count(distinct(filename)) as COUNT from voicefile_no_cache  where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600' group by from_unixtime(CAST(substring(starttime, 0, 10) as int), 'yyyy-MM-dd') order by DIS",
    "select t1.DIS DIS, t1.COUNT COUNT, t2.COUNT LENGTH from (select from_unixtime(CAST(substring(starttime, 0, 10) as int), 'yyyy-MM-dd') DIS, count(distinct(filename)) as COUNT from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000'  group by from_unixtime(CAST(substring(starttime, 0, 10) as int), 'yyyy-MM-dd')) t1 left join (select from_unixtime(CAST(substring(starttime, 0, 10) as int), 'yyyy-MM-dd') DIS, count(distinct(filename)) as COUNT from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  group by from_unixtime(CAST(substring(starttime, 0, 10) as int), 'yyyy-MM-dd')) t2 on t1.DIS = t2.DIS  order by t1.DIS",
    "select count(distinct(t1.filename)) as COUNT, SUM(bigint(t1.blanklen)) as SUM from (select filename, blanklen from acoustics_no_cache where blanklen > 0  and starttime > '1501516800000' and starttime < '1504195200000') t1 right join (select filename from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  ) t2 on t1.filename = t2.filename ",
    "select in1 DIS, count(distinct(filename)) as COUNT, AVG(duration) duration from voicefile_no_cache LATERAL VIEW explode(split(classfication, ' ')) mytab1 as in1 where length(in1) > 0 and starttime > '1501516800000' and starttime < '1504195200000'    group by in1",
    "select repGroup DIS, count(distinct(filename)) as COUNT from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  group by repGroup",
    "select hour(from_unixtime(CAST(substring(starttime, 0, 10) as int), 'yyyy-MM-dd HH:mm:ss')) DIS, count(distinct(filename)) as COUNT from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  group by hour(from_unixtime(CAST(substring(starttime, 0, 10) as int), 'yyyy-MM-dd HH:mm:ss')) order by DIS",
    "select area DIS, count(distinct(filename)) as COUNT from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  group by area",
    "select t2.DIS DIS, count(distinct(t1.filename)) as COUNT from (select filename from repeat_no_cache where starttime > '1501516800000' and starttime < '1504195200000') t1 right join (select repno DIS, filename from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  ) t2 on t1.filename = t2.filename group by t2.DIS order by COUNT desc",
    "select count(t2.filename) REPEATE from (select distinct(filename) filename from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600' ) t1 left join (select filename from repeat_no_cache where starttime > '1501516800000' and starttime < '1504195200000') t2 on t1.filename = t2.filename",
    "select t4.kw KW, count(t4.kw) WORDCOUNT, count(distinct t4.filename) FILENAMECOUNT from (select distinct(t1.filename) from (select filename from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  ) t1 left join (select filename from acoustics_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and blanklen > 0 ) t2 on t1.filename = t2.filename ) t3 left join ( select filename, kw from wordsfile_no_cache LATERAL VIEW explode(split(concat(wordsa, ' ', wordsb), ' ')) mytab1 as kw  ) t4 on t3.filename = t4.filename group by t4.kw order by WORDCOUNT desc",
    "select t2.DIS DIS, count(distinct(t1.filename)) as COUNT from (select filename from acoustics_no_cache where blanklen > 0 and starttime > '1501516800000' and starttime < '1504195200000') t1 right join (select in1 DIS, filename from voicefile_no_cache LATERAL VIEW explode(split(typeofservice, ' ')) mytab1 as in1 where length(in1) > 0  and starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  ) t2 on t1.filename = t2.filename group by t2.DIS order by COUNT desc",
    "select t2.DIS DIS, count(distinct(t1.filename)) as COUNT from (select filename from acoustics_no_cache where blanklen > 0 and starttime > '1501516800000' and starttime < '1504195200000') t1 right join(select repno DIS, filename from voicefile_no_cache where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600'  ) t2 on t1.filename = t2.filename group by t2.DIS order by COUNT desc",
    "select t2.DIS DIS, count(distinct(t1.filename)) as COUNT from (select filename from repeat_no_cache where starttime > '1501516800000' and starttime < '1504195200000') t1 right join (select in1 DIS, filename from voicefile_no_cache LATERAL VIEW explode(split(typeofservice, ' ')) mytab1 as in1 where starttime > '1501516800000' and starttime < '1504195200000' and duration > '600' and typeofservice != '' ) t2 on t1.filename = t2.filename group by t2.DIS order by COUNT desc",
    "select count(1) num from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000' and rate>='0.5') p left join (select file_name from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.file_name where s.file_name!='null'",
    "select service timeStr , count(service) num from (select v.typeofservice services from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000' and rate>='0.5' ) p left join (select file_name from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.file_name left join (select filename, typeofservice from voicefile_no_cache where starttime>='1501516800000' and starttime<='1504195199000' ) v on v.filename=p.filename where s.file_name!='null' ) LATERAL VIEW explode(split(services, ' ')) as service where service!='' group by service order by num desc",
    "select word, count(word) num from (select concat(w.wordsa,' ',w.wordsb) words from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000' and rate>='0.5' ) p left join (select filename, wordsa, wordsb from wordsfile_no_cache where starttime>='1501516800000' and starttime<='1504195199000') w on w.filename=p.filename left join (select file_name from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.file_name where s.file_name!='null' ) LATERAL VIEW explode(split(words, ' ')) as word group by word order by num desc",
    "select s.account_no, s.rep_no, s.call_number, p.filename, from_unixtime((s.start_time/1000),'yyyy-MM-dd HH:mm:ss') dateStr, v.typeofservice, concat(round(p.rate*100,3),'%') rate from (select filename,rate from predict_result where starttime>='1501516800000' and starttime<='1504195199000' and rate>='0.5' ) p inner join (select file_name filename, account_no, call_number, start_time, rep_no  from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.filename inner join (select filename , typeofservice from voicefile_no_cache where starttime>='1501516800000' and starttime<='1504195199000') v on v.filename=s.filename order by s.start_time desc",
    "select p.date timeStr, count(p.date) num from (select filename, from_unixtime((starttime/1000),'yyyy-MM-dd') date from predict_result where starttime>='1501516800000' and starttime<='1504195199000' and rate>='0.5' ) p left join (select file_name from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000'  and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.file_name where s.file_name!='null' group by p.date order by p.date",
    "select count(1) num from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000' ) p left join (select file_name from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on p.filename=s.file_name where s.file_name!='null'",
    "select s.rep_no tag , count(s.rep_no) num from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000' and rate>='0.5' ) p left join (select file_name, rep_no from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2'  ) s on s.file_name=p.filename where s.file_name!='null' group by s.rep_no order by num desc",
    "select s.rep_group timeStr, count(s.rep_group) num from (select filename from predict_result where starttime>='1501516800000' and starttime<='1504195199000' and rate>='0.5' ) p left join (select file_name, rep_group from smartv_hive_main where start_time>='1501516800000' and start_time<='1504195199000' and recorddate>='201708-0' and recorddate<='201708-2' ) s on s.file_name=p.filename where s.file_name!='null' group by s.rep_group order by num desc"
]

MODES = click.Choice(["common", "thrift"])


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
