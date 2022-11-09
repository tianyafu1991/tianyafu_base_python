#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-06-07
# @Author  : tianyafu

import configparser
import os
import sys

# 添加自定义模块到系统路径
mylib_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)) + '/utils'
sys.path.append(mylib_path)
print(mylib_path)

from logutil import Logging
import mysqlutil

logger = Logging().get_logger()

# 加载配置文件
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)) + '/config/prod.conf'
config = configparser.ConfigParser()
config.read(config_path, encoding="utf-8-sig")
front_db_section = "front_mysql"
front_db_host = config.get(front_db_section, "host")
front_db_user = config.get(front_db_section, "user")
front_db_passwd = config.get(front_db_section, "passwd")
front_db_database = config.get(front_db_section, "database")
front_db_port = config.get(front_db_section, "port")


def get_table_names(conn, db):
    sql = """
select
distinct 
tb.table_name
from
information_schema.tables tb,
information_schema.columns col
where
tb.table_schema = '%s'
and tb.table_name = col.table_name
and tb.table_schema = col.table_schema
order by
tb.table_name
    """ % db
    select_result = mysqlutil.select_with_conn(logger, conn, sql)
    result_list = []
    for tbl_name in select_result:
        result_list.append(tbl_name[0])
    return result_list


def get_table_ddl(conn, db, tables_list):
    sql_template = r"""show create table %s.%s"""
    ddl_result_list = []
    for table_name in tables_list:
        sql = sql_template % (db, table_name)
        result = mysqlutil.select_with_conn(logger, conn, sql)
        ddl_result_list.append(result[0][1])
    return ddl_result_list


def write_2_file(ddl_result_list):
    result_str = ""
    for i in ddl_result_list:
        result_str += i + ";\n-----\n"
    with open(os.path.join(r'/tmp', 'front_db_table_ddl.sql'), 'wb') as f:
        f.write(result_str.encode())
        f.flush()
        print("写出完成")


if __name__ == '__main__':
    try:
        # 获取MySQL连接
        mysql_conn = mysqlutil.connect_with_port(logger, front_db_host, front_db_user, front_db_passwd,
                                                 front_db_database, int(front_db_port))
        table_name_list = get_table_names(mysql_conn, front_db_database)
        ddl_result_list = get_table_ddl(mysql_conn, front_db_database, table_name_list)
        write_2_file(ddl_result_list)
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        mysqlutil.close(mysql_conn)
