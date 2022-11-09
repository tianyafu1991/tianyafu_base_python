#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2021-08-30
# @Author  : tianyafu

import json
import requests


headers = {"Content-Type": "application/json", "Charset": "UTF-8"}


def send_msg_2_dingding(logger, text, web_hook):
    """
    模拟请求将传入的text发送到钉钉群中
    :param logger:日志打印
    :param text:要推送的消息
    :param web_hook:钉钉的webhook
    """
    message = {"msgtype": "text", "text": {"content": text}, "at": {"atMobiles": [], "isAtAll": False}}
    message_json = json.dumps(message)
    try:
        response = requests.post(url=web_hook, headers=headers, data=message_json)
        logger.debug(response.text)
    except Exception as e:
        logger.critical('POST {}'.format(e))
