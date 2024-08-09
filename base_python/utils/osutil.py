#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-05-09
# @Author  : tianyafu


import os


def delete_files(base_path):
    """
    给定一个绝对路径 递归删除绝对路径下的所有文件和目录
    """
    for i in os.listdir(base_path):
        sub_file_abs_path = base_path + "/" + i
        # print(sub_file_abs_path)
        if os.path.isfile(sub_file_abs_path):
            os.remove(sub_file_abs_path)
        else:
            delete_files(sub_file_abs_path)


def mkdir_or_clean_dir(logger, base_path):
    """
    目标目录 存在即清空 不存在即创建
    :logger: 记录日志
    :base_path: 目标目录
    """
    if os.path.exists(base_path):
        delete_files(base_path)
        logger.info("目标目录:%s已存在 清空目标目录下的所有文件及子目录......" % base_path)
    else:
        os.mkdir(base_path)
        logger.info("目标目录:%s创建成功" % base_path)


def mkdir_dir(base_path):
    """
    给定一个绝对路径 如果目录不存在 递归创建目录
    """
    if not os.path.exists(base_path):
        os.mkdir(base_path)
        print("目标目录:%s创建成功" % base_path)
    else:
        print("目标目录:%s已存在......" % base_path)
