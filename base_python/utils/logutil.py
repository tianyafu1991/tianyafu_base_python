#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2018-08-07
# @Author  : qinpengya


import logging.config


class Logging:
    def __init__(self):
        self.logger = logging.getLogger('')

    # def get_logger(self, log_filepath):
    def get_logger(self):
        self.logger.setLevel(logging.INFO)

        # 创建一个handler，用于写入日志文件
        # fileHandler = logging.FileHandler(log_filepath)
        # fileHandler.setLevel(logging.INFO)

        # 再创建一个handler，用于输出到控制台
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.INFO)

        # 定义handler的输出格式
        formatter = logging.Formatter('%(asctime)s - %(lineno)d - %(name)s - %(levelname)s - %(message)s')
        # fileHandler.setFormatter(formatter)
        consoleHandler.setFormatter(formatter)

        # 给logger添加handler
        # self.logger.addHandler(fileHandler)
        self.logger.addHandler(consoleHandler)
        return self.logger
