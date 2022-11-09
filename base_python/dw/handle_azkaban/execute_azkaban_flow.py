#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Date    : 2022-09-23
# @Author  : tianyafu

"""
触发Azkaban的project的执行

"""

import os
import sys
import configparser
import requests

# 定义root_path
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

lib_path = root_path + '/utils'
sys.path.append(lib_path)
from logutil import Logging
import gputil
import azkabanutil

# 获取日志logger
logger = Logging().get_logger()

# 加载配置文件
config_path = root_path + '/config/prod.conf'
config = configparser.ConfigParser()
config.read(config_path, encoding="utf-8-sig")
# GP数据库相关配置
gp_section = "greenplum"
gp_host = config.get(gp_section, "host")
gp_user = config.get(gp_section, "user")
gp_passwd = config.get(gp_section, "passwd")
gp_database = config.get(gp_section, "database")
gp_port = config.get(gp_section, "port")
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


def execute_one_flow(project_name, flow, session_id, session):
    url = '%s/executor?ajax=executeFlow' % azkaban_web_url
    # post_data = {'action': 'create'}
    post_data = {}
    post_data['session.id'] = session_id
    post_data['project'] = project_name
    post_data['flow'] = flow
    logger.info("准备执行project:%s" % project_name)
    response = session.post(url, headers=azkabanutil.headers, data=post_data, verify=False)
    if response.status_code == requests.codes.ok:
        print(response)
        logger.info("成功发起执行")
    else:
        raise Exception('获取表%s的调度信息失败' % project_name)


def usage():
    print("\nThis is the usage function\n")
    print('Usage: python3 ' + sys.argv[0] + ' project_name')
    print('example: python3 execute_azkaban_flow.py zdcs_dim_warn_level_info_dd_f')


if __name__ == '__main__':
    try:
        args = sys.argv
        if len(args) != 2:
            usage()
            sys.exit(2)
        project_name = args[1].strip()
        conn = gputil.maintain_conn(logger, None, gp_host, gp_user, gp_passwd, gp_database, gp_port)
        # 从GP中获取生产环境的Azkaban的project的元数据信息 以便在测试环境创建Azkaban的project
        meta_dict = azkabanutil.get_azkaban_project_meta_from_gp(logger, conn)
        session = requests.session()
        # Azkaban Web Url 登录认证
        session_id = azkabanutil.authenticate(session, azkaban_web_user, azkaban_web_password, azkaban_web_url)
        project_meta = meta_dict.get(project_name)
        if project_meta is None:
            logger.error("该Azkaban project 不在GP表的dim_azkaban_info_dd_f表中 请执行get_azkaban_project_meta.py 刷新该表数据")
            sys.exit(2)
        execute_one_flow(project_name, project_meta.flow_id, session_id, session)
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        azkabanutil.close(session)
        gputil.close(conn)
