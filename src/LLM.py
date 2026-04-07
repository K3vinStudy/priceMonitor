# 功能：控制LLM的选择和调用

_PROMPTS_ENV = None

from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from dashscope import Generation, MultiModalConversation
from openai import OpenAI
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

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
            model='qwen-plus',
            messages=messages,
            result_format='message'
        )
        
        # 调用多模态模型需要特殊的函数
        # response = MultiModalConversation.call(
        #     model='qwen3.6-plus',
        #     messages=messages,
        #     result_format='message'
        # )

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
    llm_type = config._ENV_CACHE["LLM_TYPE"]
    api_key = config._ENV_CACHE["API_KEY"]
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

    print(f"正在处理html{url}")
    print(f"提示词：{user}")
    
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
def json2data(gid: str, thread_json: str) -> str:
    if thread_json is None:
        return -1
    
    # url = config._ENV_CACHE["PAGE_BASE_URL"] + gid
    llm_type = config._ENV_CACHE["LLM_TYPE"]
    api_key = config._ENV_CACHE["API_KEY"]
    # fetched_at = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")

    if not api_key:
        api_key = config.get_env()["api_key"]
    
    # 将 schema 文本注入 system prompt
    # schema_text = render_prompt(
    #     "2_extract/json2data_schema.json"
    # )
    system = render_prompt(
        "2_extract/json2data_system.j2",
        # schema_text=schema_text
    )
    user = render_prompt(
        "2_extract/json2data_user.j2", 
        thread_json=thread_json
    )

    print(f"正在处理json:{gid}")
    # print(f"提示词：{user}")
    
    resp = call_LLM(
        llm_type,
        api_key,
        system,
        user
    )

    # text = _extract_text_from_llm_resp(resp)
    # Debug print (optional): keep it concise
    # print(resp[:500])
    return resp







if __name__ == "__main__":
    config.get_env_cache()
    
    gid = "1858457796005892"
    in_path = Path("data/json/1_preprocess") / f"{gid}.json"
    out_path = Path("data/json/2_extract") / f"{gid}.json"
    
    with open(in_path, "r", encoding="utf-8") as f:
        json_preprocess = f.read()
    
    json_extract = json2data(gid, json_preprocess)
    
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(json_extract)

    print("saved:", out_path)