#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2021-08-24
# @Author  : tianyafu


"""
    义乌生产环境有2张表间歇性的会出现锁表现象 会导致对应的任务长时间的hang住 这种情况需要使用该监控脚本进行处理
    当出现长时间的锁表后 该脚本会自动kill掉冲突的pid
    因出现锁表时 第一步是先进行解锁 所以该脚本暂时没有在锁表情况下验证过  在通过select pg_terminate_backend('pid')去解锁时  如果解锁失败（目前还未曾出现过解锁失败的情况） 这种情况需要人为介入去判断为什么解锁失败

    目前发现经常性锁表的2张表为：事件总表(dm_work_order_info_dd_i)、企业信息表(dm_enterprise_list_dd_f)

"""

import os
import sys
import configparser

# 定义root path
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

# 添加自定义模块到系统路径
lib_path = root_path + '/utils'
sys.path.append(lib_path)
import gputil
from logutil import Logging

# 获取日志logger
logger = Logging().get_logger()

# 加载配置文件
config_path = root_path + '/config/prod.conf'
# print("配置文件路径为:%s" % config_path)
config = configparser.ConfigParser()
config.read(config_path)
gp_section = "greenplum"
gp_host = config.get(gp_section, "host")
gp_user = config.get(gp_section, "user")
gp_passwd = config.get(gp_section, "passwd")
gp_database = config.get(gp_section, "database")
gp_port = config.get(gp_section, "port")


def select_locked_pids(conn):
    """
    通过sql语句查询是否有锁表的情况 如果有锁表的 把持有锁的pid进行去重后返回
    :param conn:gp的连接
    :return:返回一个持有锁的pid的set
    """
    need_kill_pid_list = []
    # 开发脚本时 自己模拟的查询sql 只作为开发用
    #     select_sql1 = """
    #         SELECT
    # 'drop table if exists dm_enterprise_list_dd_f' as waiting_query,
    # 36062::text as w_pid,
    # 'admin'::text as w_user,
    # '<IDLE> in transaction' as locking_query,
    # 11218::text as l_pid,
    # 'admin' as l_user,
    # 'putlic.dm_enterprise_list_dd_f' as tablename
    #     """

    select_sql = """
    SELECT
w.current_query as waiting_query,
w.procpid::text as w_pid,
w.usename::text as w_user,
l.current_query as locking_query,
l.procpid::text as l_pid,
l.usename::text as l_user,
t.schemaname || '.' || t.relname as tablename
from pg_stat_activity w
join pg_locks l1 on w.procpid = l1.pid and not l1.granted
join pg_locks l2 on l1.relation = l2.relation and l2.granted
join pg_stat_activity l on l2.pid = l.procpid
join pg_stat_user_tables t on l1.relation = t.relid
    """
    result = gputil.select_with_conn(logger, select_sql, conn)
    for waiting_query, w_pid, w_user, locking_query, l_pid, l_user, tablename in result:
        logger.info(
            """当前正在等待的查询为:%s | 当前正在等待的pid和用户分别为:%s \t %s | 当前持有锁的查询为:%s | 当前持有锁的pid和用户分别为:%s \t %s | 当前被锁的表为:%s""" % (
                waiting_query, w_pid, w_user, locking_query, l_pid, l_user, tablename))
        need_kill_pid_list.append(l_pid)
    # 需要通过set进行去重
    need_kill_pid_set = set(need_kill_pid_list)
    return need_kill_pid_set


def handle_kill_pids(conn, need_kill_pid_set):
    """
    传入一个需要kill的pid的set 将set中的每个pid通过sql kill掉
    通过select pg_terminate_backend('pid') 来kill一个pid 返回的结果为True or False  True表示kill成功  False表示kill失败
    这里即使因未知返回False 好像也没有进一步的操作可以做 所以该方法中只是做了日志输出
    :param conn: gp的连接
    :param need_kill_pid_set: 需要kill的pid的set
    :return:
    """
    kill_pid_sql_template = """select pg_terminate_backend('%s')"""
    for pid in need_kill_pid_set:
        kill_pid_sql = kill_pid_sql_template % pid
        result = gputil.select_with_conn(logger, kill_pid_sql, conn)
        kill_result_flag = "成功" if result[0][0] else "失败"
        logger.info("sql:%s执行完毕......kill的结果为%s " % (kill_pid_sql, kill_result_flag))


if __name__ == '__main__':
    try:
        gp_conn = gputil.maintain_conn(logger, None, gp_host, gp_user, gp_passwd, gp_database, gp_port)
        need_kill_pid_set = select_locked_pids(gp_conn)
        if len(need_kill_pid_set) > 0:
            logger.info("有%s个pid需要被kill掉" % str(len(need_kill_pid_set)))
            handle_kill_pids(gp_conn, need_kill_pid_set)
        else:
            logger.info("没有表被锁住 无需解锁......")
    except Exception as e:
        logger.error(e)
        raise Exception
    finally:
        gputil.close(gp_conn)
