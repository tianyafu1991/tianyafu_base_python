#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Date    : 2022-09-28
# @Author  : tianyafu

"""
获取某个Azkaban project的某个job的最新的一次日志 日志长度设定为1000000000
"""

import os
import sys
import configparser
import requests
import json
from datetime import datetime

# 定义root_path
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

my_class_path = root_path + '/dw/classes'
sys.path.append(my_class_path)
# 添加自定义模块到系统路径
from AzkabanProjectMeta import AzkabanProjectMeta
from AzkabanExecution import AzkabanExecution

mylib_path = root_path + '/utils'
sys.path.append(mylib_path)
from logutil import Logging
import gputil
import azkabanutil

# 获取日志logger
logger = Logging().get_logger()

# 加载配置文件
config_path = root_path + '/config/prod.conf'
config = configparser.ConfigParser()
config.read(config_path, encoding="utf-8-sig")

gp_section = "greenplum"
gp_host = config.get(gp_section, "host")
gp_user = config.get(gp_section, "user")
gp_passwd = config.get(gp_section, "passwd")
gp_database = config.get(gp_section, "database")
gp_port = config.get(gp_section, "port")

azkaban_section = "azkaban"
azkaban_mysql_host = config.get(azkaban_section, "meta_db_host")
azkaban_mysql_user = config.get(azkaban_section, "meta_db_user")
azkaban_mysql_passwd = config.get(azkaban_section, "meta_db_passwd")
azkaban_mysql_database = config.get(azkaban_section, "meta_db_database")
azkaban_mysql_port = config.get(azkaban_section, "meta_db_port")
azkaban_web_url = config.get(azkaban_section, "web.url")
azkaban_web_user = config.get(azkaban_section, "web.user")
azkaban_web_password = config.get(azkaban_section, "web.password")


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


def fetch_last_execution_of_a_flow(project_name, flow, session_id, session):
    http_get_params = dict()
    http_get_params['ajax'] = 'fetchFlowExecutions'
    http_get_params['session.id'] = session_id
    http_get_params['project'] = project_name
    http_get_params['flow'] = flow
    http_get_params['start'] = 0
    http_get_params['length'] = 1

    url = '%s/manager' % azkaban_web_url

    response = session.get(url, params=http_get_params, headers=azkabanutil.headers, verify=False)
    if response.status_code == requests.codes.ok:
        result_json = json.loads(response.text)
        logger.info("result json is :" + str(result_json))
        execution_list = result_json.get('executions')
        project_id = result_json.get('projectId')
        azkaban_execution = AzkabanExecution()
        # 这里因为是从fetch_flow_executions.py脚本中抄过来的代码 懒得去掉这个for循环  实际上面的length参数为1 execution_list只会有最新的一次执行记录
        for execution_info in execution_list:
            submit_time = execution_info.get('submitTime')
            submit_user = execution_info.get('submitUser')
            start_time = execution_info.get('startTime')
            end_time = execution_info.get('endTime')
            exec_id = execution_info.get('execId')
            status = execution_info.get('status')
            azkaban_execution.submit_time = datetime.fromtimestamp(int(submit_time) / 1000).strftime(
                "%Y-%m-%d %H:%M:%S")
            azkaban_execution.submit_user = submit_user
            azkaban_execution.start_time = datetime.fromtimestamp(int(start_time) / 1000).strftime("%Y-%m-%d %H:%M:%S")
            azkaban_execution.end_time = datetime.fromtimestamp(int(end_time) / 1000).strftime(
                "%Y-%m-%d %H:%M:%S") if end_time != -1 else str(end_time)
            azkaban_execution.exec_id = str(exec_id)
            azkaban_execution.status = status
            logger.info("result is:\n" + str(azkaban_execution) + "\n")
        # logger.info("result is:\n" + str(list))
        # if len(result_json) != 0:
        #     flow_id = result_json.get('flows')[0].get('flowId')
        #     meta.flow_id = flow_id
        #     meta_dict[project_name] = meta
        return azkaban_execution
    else:
        raise Exception('获取azkaban project:%s的executions信息失败' % project_name)


def fetch_jobs_of_a_flow(project_name, flow, session_id, session):
    http_get_params = dict()
    http_get_params['ajax'] = 'fetchflowgraph'
    http_get_params['session.id'] = session_id
    http_get_params['project'] = project_name
    http_get_params['flow'] = flow

    url = '%s/manager' % azkaban_web_url

    response = session.get(url, params=http_get_params, headers=azkabanutil.headers, verify=False)
    if response.status_code == requests.codes.ok:
        result_json = json.loads(response.text)
        logger.info("result json is :" + str(result_json))
        execution_list = result_json.get('nodes')
        job_id_list = []
        # 这里因为是从fetch_flow_executions.py脚本中抄过来的代码 懒得去掉这个for循环  实际上面的length参数为1 execution_list只会有最新的一次执行记录
        for execution_info in execution_list:
            job_id = execution_info.get('id')
            job_id_list.append(job_id)
        # logger.info("result is:\n" + str(list))
        # if len(result_json) != 0:
        #     flow_id = result_json.get('flows')[0].get('flowId')
        #     meta.flow_id = flow_id
        #     meta_dict[project_name] = meta
        return job_id_list
    else:
        raise Exception('获取azkaban project:%s的jobIds信息失败' % project_name)


def fetch_last_execution_job_log(project_name, job_id, flow, session_id, session):
    last_azkaban_execution = fetch_last_execution_of_a_flow(project_name, flow, session_id, session)
    exec_id = last_azkaban_execution.exec_id
    job_ids = fetch_jobs_of_a_flow(project_name, flow, session_id, session)
    if job_id not in job_ids:
        raise Exception(
            '传入的Azkaban project:%s的job_id:%s不存在 该project的job_id信息为:%s' % (project_name, job_id, "、".join(job_ids)))

    http_get_params = dict()
    http_get_params['ajax'] = 'fetchExecJobLogs'
    http_get_params['session.id'] = session_id
    http_get_params['execid'] = exec_id
    http_get_params['jobId'] = job_id
    http_get_params['offset'] = 0
    http_get_params['length'] = 1000000000

    url = '%s/executor' % azkaban_web_url

    response = session.get(url, params=http_get_params, headers=azkabanutil.headers, verify=False)
    if response.status_code == requests.codes.ok:
        result_json = json.loads(response.text)
        logger.info("result json is :" + str(result_json))
    else:
        raise Exception('获取azkaban project:%s的executions信息失败' % project_name)


def usage():
    print("\nThis is the usage function\n")
    print('Usage: python3 ' + sys.argv[0] + ' project_name job_id')
    print('example: python3 fetch_last_execution_job_log.py zdcs_dim_warn_level_info_dd_f dim_warn_level_info_dd_f')


if __name__ == '__main__':
    try:
        args = sys.argv
        if len(args) != 3:
            usage()
            sys.exit(2)
        project_name = args[1].strip()
        job_id = args[2].strip()
        conn = gputil.connect_with_port(logger, gp_host, gp_user, gp_passwd, gp_database, int(gp_port))
        # 从GP中获取生产环境的Azkaban的project的元数据信息 以便在测试环境创建Azkaban的project
        meta_dict = get_azkaban_project_meta_from_gp(conn)
        session = requests.session()
        # Azkaban Web Url 登录认证
        session_id = azkabanutil.authenticate(session, azkaban_web_user, azkaban_web_password, azkaban_web_url)
        project_meta = meta_dict.get(project_name)
        if project_meta is None:
            logger.warn("该Azkaban project 不在GP表的dim_azkaban_info_dd_f表中 请执行get_azkaban_project_meta.py 刷新该表数据")
        fetch_last_execution_job_log(project_name, job_id, project_meta.flow_id, session_id, session)
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        session.close()
        gputil.close(conn)
