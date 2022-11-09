#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-10-11
# @Author  : tianyafu


from datetime import datetime
from datetime import date


def get_formatted_today(fmt):
    """
    获取今天的日期并格式化为fmt格式
    :param fmt:
    :return:
    """
    return date.today().strftime(fmt)


def validate_is_date(date_text, formatter):
    """
    验证入参date_text是否是formatter格式的日期字符串
    :param date_text:
    :param formatter:
    :return:
    """
    try:
        datetime.strptime(date_text, formatter)
        return True
    except Exception as e:
        return False
