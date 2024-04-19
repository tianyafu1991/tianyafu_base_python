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

question = "萧山区建设三路和宁东路交叉口的绿城沁桂轩即将交付，小区西门宁东路一侧临时建造的水泵房却依旧存在，垃圾堆和菜地横亘于其中，小区西门处于无法贯通的状态。 对于几百户业主来说，沁桂轩西门是通往建设三路地铁站最近的出口，而目前，该门及其紧靠的河道界面凌乱不堪，业主们堵路也堵心。 我们相信萧山区政府的效率及能力，也相信街道和社区对于辖区界面整治的决心。恳请街道及社区牵头，联合正在施工的绿城施工队，恢复绿地、贯通道路，做到还绿于民、便利于民！ 1、断头路问题：目前，沁桂轩小区西门外的道路呈现为断头路状态，无法与周边道路顺畅连接。这不仅影响了城市界面的整体美观，更对沁桂轩小区业主的日常出行造成了极大不便。 2、阻碍因素：造成这一断头路的主要原因包括街道临时水泵站的设置、人为设置不合理的围墙、垃圾山的堆积以及菜地的存在。这些障碍物严重影响了道路的连贯性和通行能力。 3. 新建高墙的懒政问题：2月26日已有居民于向信访局反馈宁东河(沁桂轩西侧)的垃圾山问题，被得到街道社区反馈开始作业清理，但3月2日居民发现该区域新建了一堵高墙，将原有的通道阻隔遮掩，与该地块在规控要求的公园绿地背道而驰。 建议与措施： 鉴于以上问题，建议街道相关部门高度重视，并采取以下措施进行整改： 1、拆除障碍物：尽快拆除街道临时水泵站、围墙及围墙边的立柱、垃圾山和菜地，为道路贯通创造条件。 2、道路改造与建设：在清除障碍物后，对断头路进行改造和建设，确保其与周边道路顺畅连接，提高通行效率。 3、绿化与美化：结合市政规划，对河边绿地进行合理规划和建设，增加绿化面积，提升城市界面的美观度。 4、责任与担当：我们对新建高墙试图掩盖问题、逃避问题、掩耳盗铃的行为强烈不满，希望街道社区能有责任担当，响应人民群众的需求，避免懒政，将持续关注并向上反馈这一问题。 请帮我给这段话提取出一句精炼的摘要。"
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

tongyi()
# tongyi_chat()
# qianfan()
# qianfan_chat()
# gemini()
# gemini_chat()