#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-09-23
# @Author  : tianyafu

"""AzkabanExecution 是Azkaban Project 的executions的抽象"""


class AzkabanExecution(object):
    def __init__(self):
        self._setup()

    def _setup(self):
        # 任务提交的时间
        self.submit_time = ''
        # 任务提交的用户
        self.submit_user = ''
        # 开始时间
        self.start_time = ''
        # 结束时间
        self.end_time = ''
        # 本次执行的id
        self.exec_id = ''
        # 执行状态
        self.status = ''

    def __str__(self):
        return "AzkabanExecution(submit_time:%s\tsubmit_user:%s\tstart_time:%s\tend_time:%s\texec_id:%s\tstatus:%s)" % (
            self.submit_time, self.submit_user, self.start_time, self.end_time, self.exec_id, self.status)
