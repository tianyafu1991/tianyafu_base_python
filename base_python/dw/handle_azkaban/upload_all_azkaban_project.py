#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-05-15
# @Author  : tianyafu

"""
上传Azkaban的project的zip包

使用场景：
当azkaban web ui 无法直接访问时 可以通过该脚本上传zip包

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
azkaban_zip_upload_abs_path = config.get(azkaban_section, "zip_upload_abs_path")
# GP数据库相关配置
gp_section = "greenplum"
gp_host = config.get(gp_section, "host")
gp_user = config.get(gp_section, "user")
gp_passwd = config.get(gp_section, "passwd")
gp_database = config.get(gp_section, "database")
gp_port = config.get(gp_section, "port")


def upload_project_zip(meta_dict, session, session_id, prod_zip_path):
    url = '%s/manager?ajax=upload' % azkaban_web_url
    post_data = {'ajax': 'upload'}
    post_data['session.id'] = session_id

    for project_name in meta_dict.keys():
        logger.info("正在上传project:%s 的zip包" % project_name)
        file_name = '%s.zip' % project_name
        zip_path = '%s/%s' % (prod_zip_path, file_name)
        zip_abs_path = os.path.abspath(zip_path)
        files = {'file': (file_name, open(zip_abs_path, 'rb'), 'application/zip')}
        # files = {'file': (file_name, open(zip_abs_path, 'rb'), 'application/zip')}
        post_data['file'] = files
        post_data['project'] = project_name
        response = session.post(url, headers=azkabanutil.headers, data=post_data, files=files, verify=False)
        if response.status_code == requests.codes.ok:
            logger.info("project:%s的zip包上传成功" % project_name)
            print(response.json())
        else:
            Exception("project:%s的zip包上传失败" % project_name)
    return meta_dict


def usage():
    print("\nThis is the usage function\n")
    print('Usage: python3 ' + sys.argv[0])


if __name__ == '__main__':
    try:
        conn = gputil.maintain_conn(logger, None, gp_host, gp_user, gp_passwd, gp_database, gp_port)
        # 从GP中获取Azkaban的project的元数据信息
        meta_dict = azkabanutil.get_azkaban_project_meta_from_gp(logger, conn)
        session = requests.session()
        # Azkaban Web Url 登录认证
        session_id = azkabanutil.authenticate(session, azkaban_web_user, azkaban_web_password, azkaban_web_url)
        upload_project_zip(meta_dict, session, session_id, azkaban_zip_upload_abs_path)
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        azkabanutil.close(session)
        gputil.close(conn)
