#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-05-09
# @Author  : tianyafu

"""TableMetaForManual 是数据库设计说明书的表元数据的抽象"""


class TableMetaForManual(object):
    def __init__(self):
        self._setup()

    def _setup(self):
        # 表名
        self.table_name = ''
        # 表注释
        self.tbl_comment = ''
        # 字段名
        self.column_name = ''
        # 字段类型
        self.column_type_name = ''
        # 字段注释
        self.column_comment = ''
        # 字段顺序
        self.integer_idx = 0
        # 允许空
        self.allow_null = ''
        # 缺省值
        self.default_value = ''

    def __str__(self):
        return "TableMetaForManual(表名:%s\t表注释:%s\t字段名:%s\t字段类型:%s\t字段注释:%s\t字段顺序:%s\t允许空:%s\t缺省值:%s)" % (
            self.table_name, self.tbl_comment, self.column_name, self.column_type_name, self.column_comment,
            self.integer_idx, self.allow_null, self.default_value
        )
