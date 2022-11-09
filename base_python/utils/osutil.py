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
