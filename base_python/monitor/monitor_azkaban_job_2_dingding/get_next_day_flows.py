#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-10-11
# @Author  : tianyafu

import os
import sys
import logging
import time
import datetime
import pickle
import requests
import configparser
from pyquery import PyQuery as pq

file_dir_abs_path = os.path.dirname(os.path.abspath(__file__))
root_abs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))


lib_path = root_abs_path + '/utils'
sys.path.append(lib_path)

import azkabanutil

logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.join(file_dir_abs_path, 'log/get_next_day_flows.log'),
                    filemode='a+',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')

requests.packages.urllib3.disable_warnings()

# 加载配置文件
config_path = root_abs_path + '/config/prod.conf'
config = configparser.ConfigParser()
config.read(config_path, encoding="utf-8-sig")
# Azkaban相关配置
azkaban_section = "azkaban"
azkaban_web_url = config.get(azkaban_section, "web_url")
azkaban_web_user = config.get(azkaban_section, "web_user")
azkaban_web_password = config.get(azkaban_section, "web_password")


def get_post_data():
    post_data = {'action': 'login'}
    post_data['username'] = azkaban_web_user
    post_data['password'] = azkaban_web_password
    return post_data


def main():
    schedule_url = '%s/schedule' % azkaban_web_url
    session = requests.session()

    try:
        # 需要修改
        post_data = get_post_data()

        response = session.post(azkaban_web_url, headers=azkabanutil.headers, data=post_data, verify=False)
        if response.status_code == 200:
            logging.info("登录成功......")
            try:
                response = session.get(schedule_url, headers=azkabanutil.headers, verify=False)
                if response.status_code == 200:
                    logging.info("爬取调度页面的信息成功......")
                    current_day = time.strftime("%Y-%m-%d", time.localtime())
                    next_day = \
                        str(datetime.datetime.strptime(current_day, '%Y-%m-%d') + datetime.timedelta(days=1)).split()[0]

                    next_day_flows_dict = {}
                    doc = pq(response.text)
                    trs = doc('tr').items()
                    for tr in trs:
                        flow = tr.find('td:nth-child(3)').text()
                        next_execution_time = tr.find('td:nth-child(7)').text()
                        if next_execution_time.startswith(next_day):
                            next_day_flows_dict[flow] = next_execution_time

                    if len(next_day_flows_dict) == 0:
                        logging.error('not crawl the scheduling page !')
                    else:
                        for k, v in next_day_flows_dict.items():
                            logging.info('flow: ' + k + ', execution time: ' + v)
                        with open(os.path.join(file_dir_abs_path, 'next_day_flows/{}.pkl'.format(next_day)), 'wb') as f:
                            pickle.dump(next_day_flows_dict, f)
                        logging.info('Crawling Success')
                else:
                    logging.error('https GET status code {}'.format(response.status_code))
            except Exception as e:
                logging.critical('GET {}'.format(e))
        else:
            logging.error('https POST status code {}'.format(response.status_code))
    except Exception as e:
        logging.critical('POST {}'.format(e))

    session.close()

    logging.info('\n')


if __name__ == '__main__':
    main()
