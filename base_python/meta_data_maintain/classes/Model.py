#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-04-12
# @Author  : tianyafu

"""Model 是模型字典目录的抽象"""


class Model(object):

    def __init__(self):
        self._setup()

    def _setup(self):
        # 编号
        self.serial_number = 0
        # 层级
        self.level = ''
        # 分类
        self.type = ''
        # 表类别
        self.table_type = ''
        # 模型名称
        self.table_name = ''
        # 模型描述
        self.table_comment = ''
        # 调度频率
        self.schedule_frequency = ''
        # azkaban调度名称
        self.schedule_name = ''
        # 脚本名称
        self.script_name = ''
        # 脚本目录
        self.script_dir = ''
        # 上线时间
        self.online_time = ''
        # 是否采集
        self.collect_flg = ''
        # 责任人
        self.principal = ''
        # 来源对应表
        self.source_table = ''
        # 来源对应表名称
        self.source_table_comment = ''
        # 增量/全量
        self.incremental_flg = ''
        # 备注
        self.remark = ''
        # 超链接对应的sheet
        self.hyperlink_sheet_name = ''
        # 行号
        self.row_no = ''

    def __str__(self):
        return "Model(行号:%s\t编号:%s\t层级:%s\t分类:%s\t表类别:%s\t模型名称:%s\t模型描述:%s\t调度频率:%s\tazkaban调度名称:%s\t脚本名称:%s\t脚本目录:%s\t上线时间:%s\t是否采集:%s\t责任人:%s\t来源对应表:%s\t来源对应表名称:%s\t是否增量:%s\t备注:%s\t超链接的sheet:%s) " % (
            self.row_no
            , self.serial_number
            , self.level
            , self.type
            , self.table_type
            , self.table_name
            , self.table_comment
            , self.schedule_frequency
            , self.schedule_name
            , self.script_name
            , self.script_dir
            , self.online_time
            , self.collect_flg
            , self.principal
            , self.source_table
            , self.source_table_comment
            , self.incremental_flg
            , self.remark
            , self.hyperlink_sheet_name
        )
