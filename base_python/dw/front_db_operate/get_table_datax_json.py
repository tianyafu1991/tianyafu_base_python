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


def get_json_template():
    template = """
    
{ 
    "job": {
        "content": [
            {
                "reader": {
                    "name": "mysqlreader", 
                    "parameter": {
                        "connection": [
                            {
                                "jdbcUrl": ["jdbc:mysql://${READER_HOST}:${READER_PORT}/${READER_DATABASE_NAME}?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8"], 
                                "querySql": ["select %s from ${READER_TABLE}"]
                            }
                        ], 
                        "password": "${READER_PASSWORD}", 
                        "username": "${READER_USERNAME}"
                    }
                }, 
                "writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": [%s], 
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://${WRITER_HOST}:${WRITER_PORT}/${WRITER_DATABASE_NAME}?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8", 
                                "table": ["${WRITER_TABLE}"]
                            }
                        ], 
                        "password": "${WRITER_PASSWORD}", 
                        "preSql": ["truncate table ${WRITER_TABLE}"], 
                        "username": "${WRITER_USERNAME}", 
                        "writeMode": "insert"
                    }
                }
            }
        ], 
        "setting": {
            "errorLimit": {
                "percentage": 0.02, 
                "record": 0
            }, 
            "speed": {
                "channel": "1"
            }
        }
    }
}
    """
    return template


def get_table_meta(conn, db):
    sql = """
    select
tb.table_name,
col.column_name,
col.data_type,
col.ordinal_position
from
information_schema.tables tb,
information_schema.columns col
where
tb.table_schema = '%s'
and tb.table_name = col.table_name
and tb.table_schema = col.table_schema
order by
tb.table_name,col.ordinal_position
    """ % db
    result = mysqlutil.select_with_conn(logger, conn, sql)
    result_dict = {}
    for (table_name, column_name, data_type, ordinal_position) in result:
        table_meta_list = result_dict.get(table_name, [])
        table_meta_list.append(column_name)
        result_dict[table_name] = table_meta_list
    return result_dict


def get_column_str(column_list):
    reader_column_str = ''
    writer_column_str = ''
    for column in column_list:
        reader_column_str += column + ','
        writer_column_str += ',\"' + column + '\"\n'
    reader_column_str_result = reader_column_str[0:-1]
    writer_column_str_result = writer_column_str[1:]
    return reader_column_str_result, writer_column_str_result


def get_json_result(table_meta_dict):
    for table_name in table_meta_dict.keys():
        column_list = table_meta_dict[table_name]
        reader_result, writer_result = get_column_str(column_list)
        json_str = get_json_template() % (reader_result, writer_result)
        with open(os.path.join(r'/tmp/json', '%s.json' % table_name), 'wb') as f:
            f.write(json_str.encode())
            f.flush()
            print("%s的json生成完成......" % table_name)


if __name__ == '__main__':
    try:
        # 获取MySQL连接
        mysql_conn = mysqlutil.connect_with_port(logger, front_db_host, front_db_user, front_db_passwd,
                                                 front_db_database, int(front_db_port))
        print("")
        result_dict = get_table_meta(mysql_conn, front_db_database)
        get_json_result(result_dict)
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        mysqlutil.close(mysql_conn)
