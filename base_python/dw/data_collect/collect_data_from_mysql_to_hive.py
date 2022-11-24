#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-11-19
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
import osutil

logger = Logging().get_logger()

# 加载配置文件
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)) + '/config/prod_dev.conf'
config = configparser.ConfigParser()
config.read(config_path, encoding="utf-8-sig")
front_db_section = "front_mysql"
front_db_host = config.get(front_db_section, "host")
front_db_user = config.get(front_db_section, "user")
front_db_passwd = config.get(front_db_section, "passwd")
front_db_database = config.get(front_db_section, "database")
front_db_port = config.get(front_db_section, "port")

project_section = 'project'
project_base_path = config.get(project_section, "base_path")
project_hive_db = config.get(project_section, "hive_db")

mysql_2_hive_data_type_dict = {
    "varchar": "string"
    , "int": "int"
    , "date": "string"
}


def get_json_template():
    template = """{
  "job": {
    "setting": {
      "speed": {
        "channel":1
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "${READER_USERNAME}",
            "password": "${READER_PASSWORD}",
            "connection": [
              {
                "querySql": [
                  "select %s,now() as create_time, now() as last_upd_time from ${READER_TABLE_NAME}"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://${READER_HOST}:${READER_PORT}/${READER_DATABASE_NAME}?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "defaultFS": "${HDFS_URL}",
            "fileType": "orc",
            "path": "${HIVE_DW_PATH}/${WRITER_TABLE_NAME}/partition_day=${PARTITION_DAY}",
            "fileName": "${WRITER_TABLE_NAME}",
            "column": [
              %s
            ],
            "writeMode": "nonConflict",
            "fieldDelimiter": "\u0001",
            "compress":"NONE"
          }
        }
      }
    ]
  }
}

    """
    return template


def get_shell_template():
    template = """#!/usr/bin/env bash

# 生效对应的环境变量
export ADMIN_PATH=/home/admin/%s
source ${ADMIN_PATH}/bin/common/common.sh

# 原始表名
SOURCE_TABLE_NAME=%s
# 目标表名
TABLE_NAME=%s

hive -e "ALTER TABLE ${DW_DB}.${TABLE_NAME} DROP IF EXISTS PARTITION (partition_day=${PARTITION_DATE})"
hive -e "ALTER TABLE ${DW_DB}.${TABLE_NAME} ADD PARTITION (partition_day=${PARTITION_DATE}) location '${HIVE_DW_PATH}/${TABLE_NAME}/partition_day=${PARTITION_DATE}'"

python ${SYSTEM_DATAX_HOME}/bin/datax.py \\
-p "-DREADER_TABLE_NAME=${SOURCE_TABLE_NAME} -DREADER_HOST=${FRONT_MYSQL_HOST} -DREADER_PORT=${FRONT_MYSQL_PORT} -DREADER_DATABASE_NAME=${FRONT_MYSQL_DATABASE_NAME} -DREADER_USERNAME=${FRONT_MYSQL_USERNAME} -DREADER_PASSWORD=${FRONT_MYSQL_PASSWORD} -DHDFS_URL=${HDFS_URL} -DHIVE_DW_PATH=${HIVE_DW_PATH} -DWRITER_TABLE_NAME=${TABLE_NAME} -DPARTITION_DAY=${PARTITION_DATE}" \\
${DW_ODS_PATH}/${TABLE_NAME}/${TABLE_NAME}.json
"""
    return template


def get_ddl_template(table_name, table_comment):
    template = f"""drop table if exists {project_hive_db}.{table_name};
create table if not exists {project_hive_db}.{table_name}(
%s,data_create_time string COMMENT '创建时间'
,data_last_upd_time string COMMENT '数据最后修改时间'
) comment '{table_comment}' 
partitioned by (partition_day string comment '日分区') 
stored as orc 
;
"""
    return template


def get_black_list():
    black_list = []
    # 这16张表已对接
    black_list.append('dm_gb_xbm')
    black_list.append('dm_gb_zjlxm')
    black_list.append('dm_gb_zzmmm')
    black_list.append('dm_hb_pyfsm')
    black_list.append('dm_rs_bmdm')
    black_list.append('t_gxgg_yktshxxb')
    black_list.append('t_gxgg_yktzhxxb')
    black_list.append('t_gxjg_jbsjzlb')
    black_list.append('t_gxts_dqjysjzlb')
    black_list.append('t_gxts_lsjysjzlb')
    black_list.append('t_gxts_smsjzlb')
    black_list.append('t_gxts_tsgmjsjb')
    black_list.append('t_gxxs_bksbjsjzlb')
    black_list.append('t_gxxs_bksbyxxb')
    black_list.append('t_gxxs_bksjbsjzlb')
    black_list.append('t_gxxs_bksxjydxxb')

    return black_list


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
    black_list = get_black_list()
    for tbl_name in select_result:
        if tbl_name[0] not in black_list:
            result_list.append(tbl_name[0])
    return result_list


def get_table_meta(conn, db, tables):
    sql = """
SELECT
b.table_name,
a.table_comment,
lower(b.column_name) AS column_name,
b.data_type,
b.column_comment,
b.ordinal_position
FROM
information_schema.TABLES a
inner join     
information_schema.`COLUMNS` b
on a.table_name = b.table_name
and a.table_schema = b.table_schema
WHERE
b.TABLE_SCHEMA = '%s'
ORDER BY
b.table_name,
b.ordinal_position
    """ % db
    result = mysqlutil.select_with_conn(logger, conn, sql)
    result_dict = {}
    for (table_name, table_comment, column_name, data_type, column_comment, ordinal_position) in result:
        if table_name in tables:
            table_meta_list = result_dict.get(table_name, [])
            table_meta_list.append((table_comment, column_name, data_type, column_comment))
            result_dict[table_name] = table_meta_list
    return result_dict


def get_datax_json_column_str(column_list):
    reader_column_str = ''
    writer_column_str = ''
    for table_comment, column_name, data_type, column_comment in column_list:
        reader_column_str += column_name + ','
        hive_data_type = mysql_2_hive_data_type_dict.get(data_type, "string")
        writer_column_str += ',{\"name\": \"%s\", \"type\": \"%s\"}\n' % (column_name, hive_data_type)
    writer_column_str += ',{\"name\": \"data_create_time\", \"type\": \"string\"}\n'
    writer_column_str += ',{\"name\": \"data_last_upd_time\", \"type\": \"string\"}'
    reader_column_str_result = reader_column_str[0:-1]
    writer_column_str_result = writer_column_str[1:]
    return reader_column_str_result, writer_column_str_result


def get_ddl_column_str(column_list):
    ddl_column_str = ''
    for table_comment, column_name, data_type, column_comment in column_list:
        hive_data_type = mysql_2_hive_data_type_dict.get(data_type, "string")
        ddl_column_str += f""",{column_name} {hive_data_type} comment '{column_comment}' \n"""
    # 去掉第一位的逗号
    ddl_column_str = ddl_column_str[1:]
    return ddl_column_str


def get_hive_table_name(tbl_name):
    return f'ods_{tbl_name}_dd_f'


def get_json_result_by_tbl_name(table_meta_dict, table_name):
    column_list = table_meta_dict[table_name]
    reader_result, writer_result = get_datax_json_column_str(column_list)
    json_str = get_json_template() % (reader_result, writer_result)
    return json_str


def get_ddl_result_by_tbl_name(table_meta_dict, table_name, hive_tbl_name):
    column_list = table_meta_dict[table_name]
    tbl_comment = column_list[0][0]
    ddl_str = get_ddl_template(hive_tbl_name, tbl_comment) % get_ddl_column_str(column_list)
    return ddl_str


def get_shell_result_by_tbl_name(hive_db, source_tbl_name, target_tbl_name):
    shell_str = get_shell_template() % (hive_db, source_tbl_name, target_tbl_name)
    return shell_str


def write_content_2_file(dir_path, target_file_name, content):
    with open(os.path.join(dir_path, target_file_name), 'wb') as f:
        f.write(content.encode())
        f.flush()
        logger.info('%s文件写入成功' % target_file_name)


def generate_ddl_json_shell_file(table_meta_dict):
    for table_name in table_meta_dict.keys():
        hive_tbl_name = get_hive_table_name(table_name)
        table_dir_path = f'{project_base_path}/{project_hive_db}/ods/{hive_tbl_name}'
        if os.path.exists(table_dir_path):
            osutil.delete_files(table_dir_path)
        else:
            os.makedirs(table_dir_path, exist_ok=True)
        datax_json_str = get_json_result_by_tbl_name(table_meta_dict, table_name)
        ddl_str = get_ddl_result_by_tbl_name(table_meta_dict, table_name, hive_tbl_name)
        shell_str = get_shell_result_by_tbl_name(project_hive_db, table_name, hive_tbl_name)
        # 写出datax的json到json文件
        write_content_2_file(table_dir_path, '%s.json' % hive_tbl_name, datax_json_str)
        # 写出ddl的sql文件
        write_content_2_file(table_dir_path, '%s.sql' % hive_tbl_name, ddl_str)
        # 写出执行采集datax脚本的shell文件
        write_content_2_file(table_dir_path, '%s.sh' % hive_tbl_name, shell_str)
        logger.info("表%s处理完成" % table_name)


if __name__ == '__main__':
    try:
        # 获取MySQL连接
        mysql_conn = mysqlutil.connect_with_port(logger, front_db_host, front_db_user, front_db_passwd,
                                                 front_db_database, int(front_db_port))
        table_name_list = get_table_names(mysql_conn, front_db_database)
        print('本次需要处理的表共%s张' % str(len(table_name_list)))

        table_meta_dict = get_table_meta(mysql_conn, front_db_database, table_name_list)

        generate_ddl_json_shell_file(table_meta_dict)
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        mysqlutil.close(mysql_conn)
        logger.info("mysql连接关闭!!!!!!!!!!!!!!!!!!!")
