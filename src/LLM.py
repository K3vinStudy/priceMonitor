# 作者：郑凯峰
# 功能：控制LLM的选择和调用

_PROMPTS_ENV = None

from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from dashscope import Generation
from openai import OpenAI
from pathlib import Path
from jinja2 import Template, Environment, FileSystemLoader

import os
import json
import dashscope

import config


# 获取变量全局缓存（env.py）
def get_prompts_env():
    global _PROMPTS_ENV
    if _PROMPTS_ENV is None:
        root_dir = Path(__file__).resolve().parents[1]
        prompts_dir = root_dir / "prompts"
        _PROMPTS_ENV = Environment(
            loader=FileSystemLoader(str(prompts_dir)),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
    return _PROMPTS_ENV


# J2模版
def get_template(relpath: str):
    # print("正在读取J2")
    return get_prompts_env().get_template(relpath)
def render_prompt(relpath: str, **kwargs) -> str:
    # print("正在渲染J2")
    return get_template(relpath).render(**kwargs)


# 功能：调用千问
def call_qwen(api_key: str, system: str, user: str):
    dashscope.api_key = api_key
    messages = [
        {'role': 'system', 'content': system},
        {'role': 'user', 'content': user}
    ]
    print("访问API...")

    try:
        response = Generation.call(
            # 可选模型：qwen-max, qwen-plus, qwen-turbo, qwen-long 等
            model='qwen3.6-plus',
            messages=messages,
            result_format='message'
        )

        if response.status_code == 200:
            # print("回答:")
            # print(response.output.choices[0].message.content)
            return response.output.choices[0].message.content
        else:
            print(f"请求失败: {response.code}, 错误信息: {response.message}")
    except Exception as e:
        print(f"发生异常: {e}")

    return 0


# 功能：调用ChatGPT
def call_gpt(api_key: str, instructions: str, input: str):
    client = OpenAI(api_key=api_key)

    resp = client.responses.create(
        model="gpt-5",
        instructions=instructions,
        input=input
    )

    return resp


# 功能：选择并调用LLM
def call_LLM(type: int, api_key: str, system: str, user: str):
    match type:
        case 1:
            resp = call_qwen(api_key, system, user)
        case 2:
            resp = call_gpt(api_key, system, user)
        case _:
            raise Exception("不支持的LLM种类")

    return resp


# 功能：根据帖子HTML文本提取页面json（目前弃用）
def html2json(gid: str, htmls: list) -> str:
    if htmls is None:
        return -1
    
    url = config._ENV_CACHE["PAGE_BASE_URL"] + gid
    llm_type = config._ENV_CACHE["llm_type"]
    api_key = config._ENV_CACHE["api_key"]
    fetched_at = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")

    if not api_key:
        api_key = config.get_env()["api_key"]
    
    # 将 schema 文本注入 system prompt
    schema_text = render_prompt(
        "1_preprocess/html2json_schema.json"
    )
    system = render_prompt(
        "1_preprocess/html2json_system.j2",
        schema_text=schema_text
    )
    user = render_prompt(
        "1_preprocess/hrmk2json_user.j2", 
        source_url=url, 
        fetched_at=fetched_at,
        html_text=htmls
    )

    print("正在处理html " + url)
    print("提示词：")
    print(user)
    
    resp = call_LLM(
        llm_type,
        api_key, 
        system, 
        user
    )
    return resp


def merge_json(jsons: list) -> str:
    resp = None
    return resp


# 功能：从帖子的json文件提取报价数据记录
def json2data(thread: str, llm_type: int, api_key: str):
    if thread is None:
        return -1

    system = (
        "你将收到一个论坛 thread 的结构化 JSON（thread/authors/nodes）。"
        "你的唯一任务是“结构还原”，即按 nodes 中的 id 与 parent_id 重建回复树，并输出可读的层级结构。"
        "\n\n约束："
        "\n1) 仅使用 JSON 中出现的节点与字段。不要推测或补全缺失节点、缺失作者信息。"
        "\n2) parent_id 表示回复关系：parent_id=null 的节点为根（通常是主楼）；其他节点挂到对应 parent_id 之下。"
        "\n3) authors 字典用于将 author_id 映射为 display_name；若不存在映射，显示为“unknown(author_id=...)”。"
        "\n4) quotes（若存在）只是引用，不是新节点，不得作为树节点输出。"
        "\n5) 若某节点的 parent_id 在本次输入中找不到，把它列入“Unresolved nodes”区，保留原 parent_id，不要猜测。"
        "\n6) 输出时必须保留每个节点的 id，方便回溯。"
    )
    user = thread

    resp = call_LLM(llm_type, api_key, system, user)
    return resp


schema_text = render_prompt("1_preprocess/url2json_schema.json")
system = render_prompt("1_preprocess/url2json_system.j2", schema_text=schema_text)
print(system)