#!/usr/bin/python3
# -*- coding: utf-8 -*-


"""
pip install langchain
pip install -U langchain-google-genai
pip install qianfan
pip install -U  dashscope
"""

from keys import *
from langchain_core.messages import HumanMessage, SystemMessage

question = "请帮我的宠物狗起一个弱爆了的名字，一定要土，还要好记"
messages = [
    SystemMessage(
        content="请一步步进行推理并得出结论。"
    ),
    HumanMessage(
        content="杂耍者有16个球，其中一半是高尔夫球，其中一半的高尔夫球是蓝色的，问一共有多少个高尔夫球是蓝色的"
    ),
]


def tongyi():
    os.environ["DASHSCOPE_API_KEY"] = DASHSCOPE_API_KEY
    from langchain_community.llms import Tongyi
    llm = Tongyi()
    print(llm.invoke(question))


def tongyi_chat():
    os.environ["DASHSCOPE_API_KEY"] = DASHSCOPE_API_KEY
    from langchain_community.chat_models.tongyi import ChatTongyi
    chat = ChatTongyi()
    print(chat.invoke(messages).content)


def qianfan():
    # pip install qianfan
    from langchain_community.llms import QianfanLLMEndpoint
    os.environ["QIANFAN_AK"] = QIANFAN_AK
    os.environ["QIANFAN_SK"] = QIANFAN_SK
    llm = QianfanLLMEndpoint(streaming=True)
    print(llm.invoke(question))


def qianfan_chat():
    from langchain_community.chat_models import QianfanChatEndpoint

    os.environ["QIANFAN_AK"] = QIANFAN_AK
    os.environ["QIANFAN_SK"] = QIANFAN_SK
    chat = QianfanChatEndpoint()
    print(chat.invoke(messages).content)


def gemini():
    from langchain_google_genai import GoogleGenerativeAI
    # 因本地无法调通接口 是网络原因 所以需要这行代码 参考https://github.com/GoogleCloudPlatform/python-docs-samples/issues/5006
    os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7897'
    os.environ["GOOGLE_API_KEY"] = GOOGLE_API_KEY
    llm = GoogleGenerativeAI(model="gemini-pro")
    print(llm.invoke(question))


def gemini_chat():
    from langchain_google_genai import ChatGoogleGenerativeAI
    os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7897'
    os.environ["GOOGLE_API_KEY"] = GOOGLE_API_KEY
    llm = ChatGoogleGenerativeAI(model="gemini-pro", convert_system_message_to_human=True)
    result = llm.invoke(messages)
    print(result.content)

# tongyi()
# tongyi_chat()
# qianfan()
# qianfan_chat()
# gemini()
# gemini_chat()