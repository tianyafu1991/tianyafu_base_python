#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-05-15
# @Author  : tianyafu

import os
import sys

# 定义root_path
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

class_path = root_path + '/dw/classes'
sys.path.append(class_path)

import requests
from AzkabanProjectMeta import AzkabanProjectMeta
import gputil

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
}


def authenticate(session, user, password, azkaban_web_url):
    """
    Azkaban Web Url 登录认证
    :param session:
    :return:
    """
    post_data = {'action': 'login'}
    post_data['username'] = user
    post_data['password'] = password
    url = '%s/index' % azkaban_web_url
    response = session.post(url, headers=headers, data=post_data, verify=False)
    if response.status_code == requests.codes.ok:
        # print(response)
        login_result_dict = eval(response.text)
        print(login_result_dict['session.id'])
        return login_result_dict['session.id']
    else:
        raise Exception('登录失败')


def authenticate2(session, user, password, azkaban_web_url):
    return '8d7e64e2-d239-4685-b6f1-35b8a5580f8c'


def close(session):
    if session is not None:
        session.close()


def get_azkaban_project_meta_from_gp(logger, conn):
    sql = """
SELECT
project_id
,project_name
,project_desc
,flow_id
,cron_expression
FROM
dim_azkaban_info_dd_f
    """
    result = gputil.select_with_conn(logger, sql, conn)
    meta_dict = dict()
    for (project_id, project_name, project_desc, flow_id, cron_expression) in result:
        meta = AzkabanProjectMeta()
        meta.project_id = project_id
        meta.project_name = project_name
        meta.project_desc = project_desc
        meta.flow_id = flow_id
        meta.cron_expression = cron_expression
        meta_dict[project_name] = meta
    return meta_dict
