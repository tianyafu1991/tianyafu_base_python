#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-05-15
# @Author  : tianyafu

"""
调度Azkaban的project

根据GP数据库的dim_azkaban_info_dd_f表中 存储的project的调度时间表达式 调度project

使用场景：
修改某个project的调度时间表达式(有时候生产环境中的Azkaban的web ui 可能无法访问 比如VPN被禁用时)

Azkaban API 参见:https://azkaban.readthedocs.io/en/latest/ajaxApi.html
requests库参见:https://docs.python-requests.org/zh_CN/latest/user/quickstart.html#id2
"""

import os
import sys
import configparser
import requests

# 定义root_path
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

my_class_path = root_path + '/dw/classes'
sys.path.append(my_class_path)
# 添加自定义模块到系统路径
from AzkabanProjectMeta import AzkabanProjectMeta

mylib_path = root_path + '/utils'
sys.path.append(mylib_path)
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
azkaban_section = "azkaban"
azkaban_mysql_host = config.get(azkaban_section, "meta_db_host")
azkaban_mysql_user = config.get(azkaban_section, "meta_db_user")
azkaban_mysql_passwd = config.get(azkaban_section, "meta_db_passwd")
azkaban_mysql_database = config.get(azkaban_section, "meta_db_database")
azkaban_mysql_port = config.get(azkaban_section, "meta_db_port")
azkaban_web_url = config.get(azkaban_section, "web.url")
azkaban_web_user = config.get(azkaban_section, "web.user")
azkaban_web_password = config.get(azkaban_section, "web.password")

gp_section = "greenplum"
gp_host = config.get(gp_section, "host")
gp_user = config.get(gp_section, "user")
gp_passwd = config.get(gp_section, "passwd")
gp_database = config.get(gp_section, "database")
gp_port = config.get(gp_section, "port")


def get_azkaban_project_meta_from_gp(conn):
    sql = """
    SELECT
	project_id,
	project_name,
	project_desc,
	flow_id,
	cron_expression
FROM
	dim_azkaban_info_dd_f
where cron_expression != ''
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


def schedule_project(meta_dict, session, session_id):
    url = '%s/schedule' % azkaban_web_url
    # post_data = {'action': 'create'}
    post_data = {}
    post_data['session.id'] = session_id
    post_data['ajax'] = 'scheduleCronFlow'

    for project_name in meta_dict.keys():
        meta = meta_dict[project_name]
        post_data['flow'] = meta.flow_id
        post_data['projectName'] = project_name
        post_data['cronExpression'] = meta.cron_expression
        logger.info("正在创建project:%s 描述为:%s" % (project_name, meta.project_desc))
        response = session.post(url, headers=azkabanutil.headers, data=post_data, verify=False)
        if response.status_code == requests.codes.ok:
            logger.info('增加project:%s的调度信息成功' % project_name)
            print(response)
        else:
            raise Exception('增加project:%s的调度信息失败' % project_name)
    return meta_dict


if __name__ == '__main__':
    try:
        conn = gputil.connect_with_port(logger, gp_host, gp_user, gp_passwd, gp_database, int(gp_port))
        # 从GP中获取生产环境的Azkaban的project的元数据信息 以便在测试环境创建Azkaban的project
        meta_dict = get_azkaban_project_meta_from_gp(conn)
        session = requests.session()
        # Azkaban Web Url 登录认证
        session_id = azkabanutil.authenticate(session, azkaban_web_user, azkaban_web_password, azkaban_web_url)
        schedule_project(meta_dict, session, session_id)
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        session.close()
        gputil.close(conn)
