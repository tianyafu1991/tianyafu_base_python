#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-05-15
# @Author  : tianyafu

"""
下载Azkaban的project

如果一个Azkaban项目没有上传过zip包 则该脚本也会下载下来一个zip包 但是这个zip包无法在windows中有压缩软件打开 会提示包损坏

Azkaban API 参见:https://azkaban.readthedocs.io/en/latest/ajaxApi.html
requests库参见:https://docs.python-requests.org/zh_CN/latest/user/quickstart.html#id2
"""

import os
import sys
import configparser
import requests
from datetime import date

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
import osutil

# 获取日志logger
logger = Logging().get_logger()

# 加载配置文件
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)) + '/config/prod.conf'
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
azkaban_zip_bak_path = config.get(azkaban_section, "zip_bak_abs_path")

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


def download_project_zip(meta_dict, session, session_id, output_path):
    url = '%s/manager' % azkaban_web_url
    http_get_params = dict()
    http_get_params['download'] = 'true'
    # http_get_params['session.id'] = session_id
    project_output_path = '%s/%s.zip'

    for project_name in meta_dict.keys():
        logger.info("正在下载project:%s 的zip包" % project_name)
        http_get_params['project'] = project_name
        project_output_abs_path = os.path.abspath(project_output_path % (output_path, project_name))
        response = session.get(url, params=http_get_params, headers=azkabanutil.headers, verify=False, stream=True)
        if response.status_code == requests.codes.ok:
            with open(project_output_abs_path, 'wb') as fd:
                for chunk in response.iter_content(chunk_size=1024):
                    fd.write(chunk)
            logger.info("project:%s 的zip包下载成功" % project_name)
        else:
            raise Exception("project:%s 的zip包下载失败" % project_name)
    return meta_dict


if __name__ == '__main__':
    try:
        zip_download_path = azkaban_zip_bak_path + '/' + date.today().strftime('%Y%m%d')
        if os.path.exists(zip_download_path):
            # 存在则递归删除该目录下的所有文件和目录
            osutil.delete_files(zip_download_path)
            logger.info("递归删除目标目录下的文件")
        else:
            # 不存在则创建
            os.mkdir(zip_download_path)
            logger.info("已创建目标目录")
        conn = gputil.connect_with_port(logger, gp_host, gp_user, gp_passwd, gp_database, int(gp_port))
        # 从GP中获取生产环境的Azkaban的project的元数据信息 以便在测试环境创建Azkaban的project
        meta_dict = get_azkaban_project_meta_from_gp(conn)
        session = requests.session()
        # Azkaban Web Url 登录认证
        session_id = azkabanutil.authenticate(session, azkaban_web_user, azkaban_web_password, azkaban_web_url)
        download_project_zip(meta_dict, session, session_id, zip_download_path)
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        session.close()
        gputil.close(conn)
