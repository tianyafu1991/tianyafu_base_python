#!/usr/bin/python3
# -*- coding: utf-8 -*-

from langchain_community.llms import Ollama
from langchain_community.chat_models import ChatOllama

llm = Ollama(model="llama2")
chat_model = ChatOllama()

from langchain_core.messages import HumanMessage, SystemMessage

text = "What would be a good company name for a company that makes colorful socks?"
messages = [
    SystemMessage(
        content="请一步步进行推理并得出结论。"
    ),
    HumanMessage(
        content="杂耍者有16个球，其中一半是高尔夫球，其中一半的高尔夫球是蓝色的，问一共有多少个高尔夫球是蓝色的"
    ),
]
# print(llm.invoke(text))
print(chat_model.invoke(messages))
