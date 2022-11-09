#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2021-08-30
# @Author  : tianyafu


"""
    统计表的数据量的脚本 并将统计结果发送到钉钉
"""

import os
import sys
import configparser

import datetime

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

# 添加自定义模块到系统路径
mylib_path = root_path + '/utils'
sys.path.append(mylib_path)
import gputil
from logutil import Logging
import dingdingutil

# 获取日志logger
logger = Logging().get_logger()

# 加载配置文件
config_path = root_path + '/config/prod.conf'
# print("配置文件路径为:%s" % config_path)
config = configparser.ConfigParser()
config.read(config_path)
gp_section = "greenplum"
gp_host = config.get(gp_section, "host")
gp_user = config.get(gp_section, "user")
gp_passwd = config.get(gp_section, "passwd")
gp_database = config.get(gp_section, "database")
gp_port = config.get(gp_section, "port")

dingding_section = "dingding"
dingding_web_hook = config.get(dingding_section, "webhook")
print("~~~~~~~~~~~钉钉的webhook：" + dingding_web_hook)

today = datetime.datetime.today().strftime('%Y-%m-%d')
yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
print("今天为:%s 昨天为:%s" % (today, yesterday))


def get_table_count(table_names, conn):
    """
    获取table_names中每个表的数据条数
    :param table_names:一个list 里面有需要监控的表名
    :param conn:gp的连接
    :return:返回一个表名 数据条数的字典
    """
    select_sql_template = """
        select count(1) cnt from %s
    """
    get_table_meta_sql_template = """
    SELECT A.SCHEMANAME AS SCHEMANAME,
       A.TABLENAME AS TABLENAME,
       obj_description(b.oid),
       D.ATTNAME AS ATTNAME,
       REPLACE(REPLACE(REPLACE(FORMAT_TYPE(D.ATTTYPID, D.ATTTYPMOD),'numeric','NUMBER'),'character varying','VARCHAR'),'date','DATE') AS DATA_TYPE,
       E.DESCRIPTION
  FROM PG_TABLES A
 INNER JOIN PG_CLASS B
    ON A.TABLENAME = B.RELNAME
  LEFT JOIN PG_CATALOG.PG_DESCRIPTION E
    ON B.OID = E.OBJOID
  LEFT JOIN PG_CATALOG.PG_ATTRIBUTE D
    ON D.ATTRELID = E.OBJOID
   AND D.ATTNUM = E.OBJSUBID
 WHERE SCHEMANAME = 'public'
   AND A.TABLENAME LIKE '%s'
   AND D.ATTNUM > 0
  ORDER BY A.TABLENAME ,D.ATTNUM
  limit 1
    """

    table_cnt_dict = {}
    for table_name in table_names:
        select_sql = select_sql_template % table_name
        result = gputil.select_with_conn(logger, select_sql, conn)
        table_meta_result = gputil.select_with_conn(logger, get_table_meta_sql_template % table_name, conn)
        table_desc = ''
        if len(table_meta_result) > 0:
            table_desc = table_meta_result[0][2]
        cnt = result[0][0]
        dict_value = (table_name, table_desc, str(cnt))
        table_cnt_dict[table_name] = dict_value
        print("表注释为:%s 表%s的数据条数为:%s" % (table_desc, table_name, str(cnt)))
    return table_cnt_dict


def get_statistics_info(logger, conn, tbl_name, day):
    select_sql = """
    select table_name,table_cnt from statistics_table_meta_2_dingding where table_name = '%s' and statistics_date = '%s' order by create_time desc
    """ % (tbl_name, day)
    yesterday_result = gputil.select_with_conn(logger, select_sql, conn)
    cnt = 0
    if len(yesterday_result) > 0:
        cnt = int(yesterday_result[0][1])
    logger.info("表%s昨天的数据量为%s" % (tbl_name, str(cnt)))
    return cnt


def insert__statistics_info(logger, conn, data):
    insert_sql = """
    insert into statistics_table_meta_2_dingding(table_name,table_desc,table_cnt,statistics_date,ratio) 
values('%s','%s','%s','%s','%s')
    """ % (data[0], data[1], data[2], data[3], data[4])
    gputil.insert(logger, insert_sql, data, conn)
    logger.info("今天的统计结果插入GP成功")


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("参数异常")
        sys.exit(2)
    else:
        try:
            # 获取表名列表  表名可以传多个 用空格分隔
            table_names = sys.argv[1:]
            gp_conn = gputil.maintain_conn(logger, None, gp_host, gp_user, gp_passwd, gp_database, gp_port)
            table_cnt_dict = get_table_count(table_names, gp_conn)

            res = '【XX项目】{}【XX项目生产环境监控】'.format(today)
            for key in table_cnt_dict:
                yesterday_cnt = get_statistics_info(logger, gp_conn, key, yesterday)
                dict_value = table_cnt_dict[key]
                table_name = dict_value[0]
                table_desc = dict_value[1]
                table_cnt = dict_value[2]
                ratio = 0
                if yesterday_cnt != 0:
                    ratio = round(int(table_cnt) * 1.0 / yesterday_cnt, 4)
                table_name_2 = "%s(%s)" % (table_desc, table_name)
                insert_data = (table_name, table_desc, table_cnt, today, ratio)
                insert__statistics_info(logger, gp_conn, insert_data)
                table_msg = "表名: " + table_name_2 + " 表数据条数为:" + str(table_cnt) + " 与昨天数据量的比值为:" + str(ratio)
                res = '\n'.join([res, table_msg])
            # print(res)
            dingdingutil.send_msg_2_dingding(logger, res, dingding_web_hook)
        except Exception as e:
            logger.error(e)
            raise Exception
        finally:
            gputil.close(gp_conn)
            logger.info("GP数据库连接已关闭......")
