#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/3/21 11:26
# @Author  : Qin
# @Site    : 
# @File    : yamlutil.py


import yaml
import os

def get_source(db_type):
    """
    Get database connection information
    :param db_type: database type. pgsql, mysql, hive, hmysql
    :return: connnection info: host user password, database, port
    """
    # on windows
    # yaml_info = yaml.load(open(''))
    # print(yaml_info)
    # on linux
    current_path = os.path.abspath(__file__)
    parent_path = os.path.dirname(current_path)
    conf_path = os.path.dirname(parent_path) + '/config/conf.yaml'
    yaml_info = yaml.load(open(conf_path))
    source_info = yaml_info['source']
    db_info = source_info[db_type]
    if db_info is None or len(db_info) < 1:
        return "'%s' cannot find in 'conf.yaml'" % db_type
    host, user, passwd, database, port = \
        db_info["host"], db_info["user"], db_info["passwd"], db_info["database"], db_info["port"]
    return host, user, passwd, database, port