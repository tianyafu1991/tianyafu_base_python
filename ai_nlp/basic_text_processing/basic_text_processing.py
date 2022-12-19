#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-12-16
# @Author  : tianyafu


def str_strip():
    en_str = " ~hello world, my name is tianyafu!, "
    en_str_1 = en_str.strip()
    # 左边去空格  右边去空格和逗号
    en_str_2 = en_str.lstrip().rstrip(" ,")
    print(en_str_1)
    print(en_str_2)


def str_replace():
    en_str = " ~hello world, my name is tianyafu!, hello tianyafu"
    en_str_1 = en_str.replace("hello", "hi")
    print(en_str_1)


def str_substring():
    my_str = "大家好，我是李雪琴，我在北京大学，你吃饭没呢？"
    # 切片索引从0开始 左闭右开
    my_str_1 = my_str[0:3]
    my_str_2 = my_str[4:4 + 5]
    # 从右往左时 索引从-1开始 也是左闭右开
    my_str_3 = my_str[-1 - 5:-1]
    # 间隔截取
    my_str_4 = my_str[::2]
    # 反转
    my_str_5 = my_str[::-1]
    print(my_str_1)
    print(my_str_2)
    print(my_str_3)
    print(my_str_4)
    print(my_str_5)


def str_join_and_split():
    str1 = "大家好，我是陆超，真好！"
    str2 = "大家好，我是李雪琴，我在北京大学，你吃饭没呢？"
    str3 = str1 + str2
    print(str3)
    strs = ["大家好，我是陆超，真好！", "大家好，我是李雪琴，我在北京大学，你吃饭没呢？", "我是！！！！！！！！！！"]
    str4 = "~~~~~~~~~~~~~~".join(strs)
    print(str4)
    strs2 = str4.split("~~~~~~~~~~~~~~")
    print(strs2, type(strs2))



def sort_fun(x):
    return x[1].lower()

def str_compare_and_order():
    en_strs = ['ABc', 'aCd', 'CdE', 'xYz']
    en_strs_2 = sorted(en_strs)
    en_strs_3 = sorted(en_strs,key=sort_fun)
    print(en_strs)
    print(en_strs_2)
    print(en_strs_3)


if __name__ == '__main__':
    # str_strip()
    # str_replace()
    # str_substring()
    # str_join_and_split()
    str_compare_and_order()
