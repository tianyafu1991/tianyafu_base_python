#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-05-15
# @Author  : tianyafu

"""
获取Azkaban的project的元数据 并写入GP的表中

Azkaban API 参见:https://azkaban.readthedocs.io/en/latest/ajaxApi.html
requests库参见:https://docs.python-requests.org/zh_CN/latest/user/quickstart.html#id2
"""

import os
import sys
import configparser
import requests
import json
from pyquery import PyQuery as pq
import pandas as pd

# 定义root_path
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

my_class_path = root_path + '/dw/classes'
sys.path.append(my_class_path)
# 添加自定义模块到系统路径
from AzkabanProjectMeta import AzkabanProjectMeta

lib_path = root_path + '/utils'
sys.path.append(lib_path)
from logutil import Logging
import gputil
import azkabanutil

# 获取日志logger
logger = Logging().get_logger()

# 加载配置文件
config_path = root_path + '/config/prod.conf'
# print("配置文件路径为:%s" % config_path)
config = configparser.ConfigParser()
config.read(config_path, encoding="utf-8-sig")
# Azkaban相关配置
azkaban_section = "azkaban"
azkaban_mysql_host = config.get(azkaban_section, "meta_db_host")
azkaban_mysql_user = config.get(azkaban_section, "meta_db_user")
azkaban_mysql_passwd = config.get(azkaban_section, "meta_db_passwd")
azkaban_mysql_database = config.get(azkaban_section, "meta_db_database")
azkaban_mysql_port = config.get(azkaban_section, "meta_db_port")
azkaban_web_url = config.get(azkaban_section, "web_url")
azkaban_web_user = config.get(azkaban_section, "web_user")
azkaban_web_password = config.get(azkaban_section, "web_password")
# GP数据库相关配置
gp_section = "greenplum"
gp_host = config.get(gp_section, "host")
gp_user = config.get(gp_section, "user")
gp_passwd = config.get(gp_section, "passwd")
gp_database = config.get(gp_section, "database")
gp_port = config.get(gp_section, "port")

truncate_sql = """truncate table %s"""


def fetch_user_project(session, session_id):
    http_get_params = dict()
    http_get_params['ajax'] = 'fetchuserprojects'
    http_get_params['session.id'] = session_id
    url = '%s/index' % azkaban_web_url
    response = session.get(url, params=http_get_params, headers=azkabanutil.headers, verify=False)
    result_dict = dict()
    if response.status_code == requests.codes.ok:
        user_project_result_dict = eval(response.text)
        for project_info in user_project_result_dict['projects']:
            project_id = project_info['projectId']
            project_name = project_info['projectName']
            meta = AzkabanProjectMeta()
            meta.project_name = project_name
            meta.project_id = project_id
            result_dict[project_name] = meta
        return result_dict
    else:
        raise Exception('获取用户的project失败')


def fetch_flows_of_project(meta_dict, session, session_id):
    http_get_params = dict()
    http_get_params['ajax'] = 'fetchprojectflows'
    http_get_params['session.id'] = session_id
    url = '%s/manager' % azkaban_web_url
    for project_name in meta_dict.keys():
        meta = meta_dict[project_name]
        http_get_params['project'] = project_name
        response = session.get(url, params=http_get_params, headers=azkabanutil.headers, verify=False)
        if response.status_code == requests.codes.ok:
            result_json = json.loads(response.text)
            if len(result_json) != 0:
                if len(result_json.get('flows')) == 0:
                    # 如果一个Azkaban项目 没有上传zip包 则此时只是一个空项目  不会有flowId 所以直接给空串
                    flow_id = ''
                else:
                    flow_id = result_json.get('flows')[0].get('flowId')
                meta.flow_id = flow_id
                meta_dict[project_name] = meta
        else:
            raise Exception('获取表%s的flow信息失败' % project_name)
    return meta_dict


def fetch_schedule(meta_dict, session, session_id):
    """
    获取调度信息
    :param meta_dict:
    :param session:
    :param session_id:
    :return:
    """
    url = '%s/schedule' % azkaban_web_url
    http_get_params = dict()
    http_get_params['ajax'] = 'fetchSchedule'
    http_get_params['session.id'] = session_id
    for project_name in meta_dict.keys():
        meta = meta_dict[project_name]
        http_get_params['projectId'] = meta.project_id
        http_get_params['flowId'] = meta.flow_id
        response = session.get(url, params=http_get_params, headers=azkabanutil.headers, verify=False)
        if response.status_code == requests.codes.ok:
            result_json = json.loads(response.text)
            # 一个project可能并没有进行调度 所以这里要判断长度是否为0
            if len(result_json) != 0:
                cron_expression = result_json.get('schedule').get('cronExpression')
                meta.cron_expression = cron_expression
                meta_dict[project_name] = meta
        else:
            raise Exception('获取表%s的调度信息失败' % project_name)
    return meta_dict


def get_project_description(meta_dict, session, session_id):
    http_get_params = dict()
    # http_get_params['ajax'] = 'fetchSchedule'
    http_get_params['session.id'] = session_id

    url = '%s/manager' % azkaban_web_url
    for project_name in meta_dict.keys():
        meta = meta_dict[project_name]
        http_get_params['project'] = project_name
        response = session.get(url, params=http_get_params, headers=azkabanutil.headers, verify=False)
        if response.status_code == requests.codes.ok:
            doc = pq(response.text)
            meta.project_desc = doc("p[id^=project-description]").text()
            meta_dict[project_name] = meta
    return meta_dict


def convert_2_df(meta_dict, columns):
    metas = []
    for project_name in meta_dict:
        meta = meta_dict[project_name]
        metas.append((meta.project_id, meta.project_name, meta.project_desc, meta.flow_id, meta.cron_expression))
    return pd.DataFrame.from_records(metas, columns=columns)


if __name__ == '__main__':
    try:
        table_name = 'dim_azkaban_info_dd_f'
        columns = ['project_id', 'project_name', 'project_desc', 'flow_id', 'cron_expression']
        conn = gputil.maintain_conn(logger, None, gp_host, gp_user, gp_passwd, gp_database, gp_port)
        session = requests.session()
        # Azkaban Web Url 登录认证
        session_id = azkabanutil.authenticate(session, azkaban_web_user, azkaban_web_password, azkaban_web_url)
        meta_dict = fetch_user_project(session, session_id)
        meta_dict = fetch_flows_of_project(meta_dict, session, session_id)
        meta_dict = fetch_schedule(meta_dict, session, session_id)
        meta_dict = get_project_description(meta_dict, session, session_id)
        meta_df = convert_2_df(meta_dict, columns)
        gputil.truncate(logger, truncate_sql % table_name, conn)
        gputil.df_2_gp_use_copy_from(logger, meta_df, conn, table_name, columns, "|")
        print("Azkaban元数据信息收集成功......")
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        azkabanutil.close(session)
        gputil.close(conn)
