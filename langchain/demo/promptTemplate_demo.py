#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
https://python.langchain.com/docs/modules/model_io/prompts/quick_start
"""

from keys import *
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_community.chat_models.tongyi import ChatTongyi
from langchain.prompts import PromptTemplate

template = """
你是一位专业的美食推荐文案撰写员，对于售价为{price}元的{product}，请写一份非常吸引人的卖点文案
"""
prompt_template = PromptTemplate.from_template(
    template
)
prompt = prompt_template.format(price="198", product="车厘子")

os.environ["DASHSCOPE_API_KEY"] = DASHSCOPE_API_KEY
chat = ChatTongyi()
print(chat.invoke(prompt).content.strip())