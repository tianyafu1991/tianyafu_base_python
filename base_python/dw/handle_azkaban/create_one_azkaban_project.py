#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-05-15
# @Author  : tianyafu

"""
创建Azkaban的project

使用场景:
当Azkaban的web ui 无法访问时 需要新建一个Azkaban project

Azkaban API 参见:https://azkaban.readthedocs.io/en/latest/ajaxApi.html
requests库参见:https://docs.python-requests.org/zh_CN/latest/user/quickstart.html#id2
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


def create_one_project(project_name, project_desc, session, session_id):
    url = '%s/manager?action=create' % azkaban_web_url
    # post_data = {'action': 'create'}
    post_data = {}
    post_data['session.id'] = session_id
    post_data['description'] = project_desc
    post_data['name'] = project_name

    logger.info("正在创建project:%s 描述为:%s" % (project_name, project_desc))
    response = session.post(url, headers=azkabanutil.headers, data=post_data, verify=False)
    if response.status_code == requests.codes.ok:
        print(response)
        logger.info("创建成功......")
    else:
        raise Exception('获取表%s的调度信息失败' % project_name)


def usage():
    print("\nThis is the usage function\n")
    print('Usage: python3 ' + sys.argv[0] + ' project_name project_desc')


if __name__ == '__main__':
    try:
        args = sys.argv
        if len(args) != 3:
            usage()
            sys.exit(2)

        project_name = args[1].strip()
        project_desc = args[2].strip()
        conn = gputil.maintain_conn(logger, None, gp_host, gp_user, gp_passwd, gp_database, gp_port)
        session = requests.session()
        # 从GP中获取Azkaban的project的元数据信息
        meta_dict = azkabanutil.get_azkaban_project_meta_from_gp(logger, conn)
        if meta_dict.get(project_name) is not None:
            logger.error("Azkaban项目名称已存在!!!!!!!!")
            sys.exit(2)
        # Azkaban Web Url 登录认证
        session_id = azkabanutil.authenticate(session, azkaban_web_user, azkaban_web_password, azkaban_web_url)
        create_one_project(project_name, project_desc, session, session_id)
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        azkabanutil.close(session)
        gputil.close(conn)
