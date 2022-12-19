#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-12-16
# @Author  : tianyafu

import re


def re_1():
    pattern = re.compile(r'hello.*\!')
    match = pattern.match('hello, hanxiaoyang! How are you? hello, tianyafu!')
    match2 = pattern.match('hello, hanxiaoyang! How are you?')
    if match:
        print(match.group())
        print("~~~~~~~~~~~~~~")
        print(match.string)

    if match2:
        print("~~~~~~~~~~~~~~")
        print(match2.group())


def re_2():
    m = re.match(r'(\w+)(\w+)(?P<sign>.*)', 'hello hanxiaoyang!')
    print("m.string:", m.string)
    print("m.re:", m.re)
    print("m.pos:", m.pos)
    print("m.endpos:", m.endpos)
    print("m.lastindex:", m.lastindex)
    print("m.lastgroup:", m.lastgroup)

    print("m.group(1,2):", m.group(1, 2))
    print("m.groups():", m.groups())
    print("m.groupdict():", m.groupdict())
    print("m.start(2):", m.start(2))
    print("m.end(2):", m.end(2))
    print("m.span(2):", m.span(2))
    print(r"m.expand(r'\2 \1\3'):", m.expand(r'\2 \1\3'))


def re_split():
    p = re.compile(r'\d+')
    print(p.split('one1two2three3four4'))


def re_sub():
    p = re.compile(r'(\w+) (\w+)')
    s = 'i say, hello hanxiaoyang!'
    print(p.sub(r'\2 \1',s))


if __name__ == '__main__':
    # re_1()
    # re_2()
    # re_split()
    re_sub()
