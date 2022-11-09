#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-09-27
# @Author  : tianyafu

"""
定期删除不需要的Azkaban zip包备份

生产环境Azkaban的zip包是每日定期备份的 时间长了会导致备份目录下的子目录太多
目前设定该目录下 只保留过去30天的备份 所以需要定期删除不需要的备份

"""
import os
import sys
import configparser

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

lib_path = root_path + '/utils'
sys.path.append(lib_path)
from logutil import Logging
import osutil
import datetimeutil

# 获取日志logger
logger = Logging().get_logger()

# 加载配置文件
config_path = root_path + '/config/prod.conf'
# print("配置文件路径为:%s" % config_path)
config = configparser.ConfigParser()
config.read(config_path, encoding="utf-8-sig")
azkaban_section = "azkaban"
azkaban_zip_bak_path = config.get(azkaban_section, "zip_bak_abs_path")
zip_bak_store_days = int(config.get(azkaban_section, "zip_bak_store_days"))


def usage():
    print("\nThis is the usage function\n")
    print('Usage: python3 ' + sys.argv[0])


if __name__ == '__main__':
    sub_file_list = os.listdir(azkaban_zip_bak_path)
    zip_bak_dir_list = []
    for file_name in sub_file_list:
        if datetimeutil.validate_is_date(file_name, '%Y%m%d'):
            zip_bak_dir_list.append(file_name)
    if len(zip_bak_dir_list) > zip_bak_store_days:
        # 如果备份的目录超过预留的个数 则删除较早的备份目录 防止备份目录过多
        # 按照时间排序
        zip_bak_dir_list.sort()
        # 获取到需要删除的目录list
        need_delete_dir_list = zip_bak_dir_list[0:len(zip_bak_dir_list) - zip_bak_store_days]
        for i in need_delete_dir_list:
            # 绝对路径
            need_delete_dir_abs_path = '%s/%s' % (azkaban_zip_bak_path, i)
            print("准备删除%s" % need_delete_dir_abs_path)
            # 递归删除该目录下的子目录或子文件
            osutil.delete_files(need_delete_dir_abs_path)
            # 删除该目录自身
            os.removedirs(need_delete_dir_abs_path)
            logger.info("目录%s删除成功" % need_delete_dir_abs_path)
    else:
        # 如果备份的目录没有超过预留的个数  则不做额外处理
        logger.info("当天没有需要删除的目录")
