#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-05-09
# @Author  : tianyafu

"""AzkabanProjectMeta 是Azkaban Project元数据的抽象"""


class AzkabanProjectMeta(object):
    def __init__(self):
        self._setup()

    def _setup(self):
        # 项目id
        self.project_id = ''
        # 项目名
        self.project_name = ''
        # 项目描述
        self.project_desc = ''
        # 工作流id
        self.flow_id = ''
        # 调度的时间表达式
        self.cron_expression = ''


    def __str__(self):
        return "AzkabanProjectMeta(项目id:%s\t项目名:%s\t项目描述:%s\t工作流id:%s\t调度的时间表达式:%s)" % (
            self.project_id, self.project_name,self.project_desc, self.flow_id, self.cron_expression)
