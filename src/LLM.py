# 功能：控制LLM的选择和调用

_PROMPTS_ENV = None

from datetime import datetime
import json
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from dashscope import Generation, MultiModalConversation
from openai import OpenAI, APITimeoutError, APIError
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import logging

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


def _emit_log(log, message: str):
    if log is not None:
        log(message)
    else:
        print(message)


# J2模版
def get_template(relpath: str):
    # print("正在读取J2")
    return get_prompts_env().get_template(relpath)
def render_prompt(relpath: str, **kwargs) -> str:
    # print("正在渲染J2")
    return get_template(relpath).render(**kwargs)


# 功能：调用千问
def call_qwen(api_key: str, system: str, user: str, should_stop=None, log=None):
    if should_stop and should_stop():
        return "{}"

    dashscope.api_key = api_key
    env = config.get_env_cache()
    model = env['MODEL_TYPE']
    messages = [
        {'role': 'system', 'content': system},
        {'role': 'user', 'content': user}
    ]

    timeout_seconds = 180
    max_retries = 2  # 重试机会：2 次（总共最多 3 次尝试）
    logger = logging.getLogger(__name__)

    def _do_call():
        # return MultiModalConversation.call(
        #     model=model,
        #     messages=messages,
        #     result_format='message'
        # )
        
        return Generation.call(
            model=model,
            messages=messages,
            result_format="message",
        )

    def render_content(content: list) -> str:
        text_parts = []
        for item in content:
            if isinstance(item, dict) and "text" in item:
                text_parts.append(item["text"])
            else:
                text_parts.append(str(item))
        return "\n".join(text_parts)

    for attempt in range(max_retries + 1):
        if should_stop and should_stop():
            return "{}"

        _emit_log(log, f"访问 DashScope 接口... 第 {attempt + 1} 次尝试, model={model}")
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(_do_call)
        try:
            response = future.result(timeout=timeout_seconds)

            if should_stop and should_stop():
                return "{}"

            if response.status_code == 200:
                content = response.output.choices[0].message.content
                if isinstance(content, str):
                    return content
                if isinstance(content, list):
                    return render_content(content)
                return str(content)
            else:
                raise RuntimeError(f"请求失败: {response.code}, 错误信息: {response.message}")

        except FuturesTimeoutError as e:
            future.cancel()
            _emit_log(log, f"DashScope 接口超时（>{timeout_seconds}秒）: 第 {attempt + 1} 次尝试失败")
            if should_stop and should_stop():
                return "{}"
            if attempt < max_retries:
                _emit_log(log, "准备重试...")
                continue
            logger.exception(
                "DashScope 接口超时，已达到重试上限。timeout_seconds=%s, max_retries=%s",
                timeout_seconds,
                max_retries,
                exc_info=e,
            )
            _emit_log(log, "DashScope 接口调用超时，已达到重试上限")
            return "{}"

        except Exception as e:
            _emit_log(log, f"DashScope 接口异常: 第 {attempt + 1} 次尝试失败, {e}")
            if should_stop and should_stop():
                return "{}"
            if attempt < max_retries:
                _emit_log(log, "准备重试...")
                continue
            logger.exception("DashScope 接口调用异常，已达到重试上限")
            _emit_log(log, "DashScope 接口调用异常，已达到重试上限")
            return "{}"

        finally:
            executor.shutdown(wait=False, cancel_futures=True)


# 功能：使用OpenAI兼容接口调用千问
def call_qwen_openai_compat(api_key: str, system: str, user: str, should_stop=None, log=None):
    if should_stop and should_stop():
        return "{}"

    env = config.get_env_cache()
    model = env['MODEL_TYPE']
    base_url = env.get('QWEN_OPENAI_BASE_URL') or "https://dashscope.aliyuncs.com/compatible-mode/v1"
    timeout_seconds = 180
    max_retries = 2  # 重试机会：2 次（总共最多 3 次尝试）
    logger = logging.getLogger(__name__)

    client = OpenAI(
        api_key=api_key,
        base_url=base_url,
        timeout=timeout_seconds,
    )

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]

    def render_content(content) -> str:
        if content is None:
            return "{}"
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            text_parts = []
            for item in content:
                if isinstance(item, dict):
                    if "text" in item:
                        text_parts.append(item["text"])
                    else:
                        text_parts.append(str(item))
                else:
                    text_parts.append(str(item))
            return "\n".join(text_parts)
        return str(content)

    for attempt in range(max_retries + 1):
        if should_stop and should_stop():
            return "{}"

        _emit_log(log, f"访问 OpenAI 兼容接口... 第 {attempt + 1} 次尝试, model={model}")
        try:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0,
            )

            if should_stop and should_stop():
                return "{}"

            if not response.choices:
                raise RuntimeError("响应中没有 choices")

            message = response.choices[0].message
            content = getattr(message, "content", None)
            return render_content(content)

        except APITimeoutError as e:
            _emit_log(log, f"OpenAI 兼容接口调用超时（>{timeout_seconds}秒）: 第 {attempt + 1} 次尝试失败")
            if should_stop and should_stop():
                return "{}"
            if attempt < max_retries:
                _emit_log(log, "准备重试...")
                continue
            logger.exception(
                "OpenAI兼容接口调用超时，已达到重试上限。timeout_seconds=%s, max_retries=%s",
                timeout_seconds,
                max_retries,
                exc_info=e,
            )
            _emit_log(log, "OpenAI 兼容接口调用超时，已达到重试上限")
            return "{}"

        except APIError as e:
            _emit_log(log, f"OpenAI 兼容接口异常: 第 {attempt + 1} 次尝试失败, {e}")
            if should_stop and should_stop():
                return "{}"
            if attempt < max_retries:
                _emit_log(log, "准备重试...")
                continue
            logger.exception("OpenAI兼容接口调用异常，已达到重试上限", exc_info=e)
            _emit_log(log, "OpenAI 兼容接口调用异常，已达到重试上限")
            return "{}"

        except Exception as e:
            _emit_log(log, f"OpenAI 兼容接口异常: 第 {attempt + 1} 次尝试失败, {e}")
            if should_stop and should_stop():
                return "{}"
            if attempt < max_retries:
                _emit_log(log, "准备重试...")
                continue
            logger.exception("OpenAI兼容接口调用出现未知异常，已达到重试上限", exc_info=e)
            _emit_log(log, "OpenAI 兼容接口调用异常，已达到重试上限")
            return "{}"


# 功能：调用ChatGPT
def call_gpt(api_key: str, instructions: str, input: str, should_stop=None, log=None):
    if should_stop and should_stop():
        return "{}"

    env = config.get_env_cache()
    model = env['MODEL_TYPE']
    client = OpenAI(api_key=api_key)

    _emit_log(log, f"访问 OpenAI 接口... model={model}")
    resp = client.responses.create(
        model=model,
        instructions=instructions,
        input=input
    )

    if should_stop and should_stop():
        return "{}"

    return resp.output_text if hasattr(resp, 'output_text') else str(resp)


# 功能：选择并调用LLM
def call_LLM(type: int, api_key: str, system: str, user: str, should_stop=None, log=None):
    if should_stop and should_stop():
        return "{}"

    match type:
        case 1:
            resp = call_qwen_openai_compat(api_key, system, user, should_stop=should_stop, log=log)
            # resp = call_qwen(api_key, system, user, should_stop=should_stop, log=log)
        case 2:
            resp = call_gpt(api_key, system, user, should_stop=should_stop, log=log)
        case _:
            raise Exception("不支持的LLM种类")

    return resp


# 功能：根据帖子HTML文本提取页面json（目前弃用）
def html2json(gid: str, htmls: list, should_stop=None, log=None) -> str:
    if htmls is None:
        return -1
    if should_stop and should_stop():
        return "{}"

    env = config.get_env_cache()
    url = env["PAGE_BASE_URL"] + gid
    llm_type = env["LLM_TYPE"]
    api_key = env["API_KEY"]
    fetched_at = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")

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

    _emit_log(log, f"正在处理html{url}")
    _emit_log(log, f"提示词：{user}")

    if should_stop and should_stop():
        return "{}"

    resp = call_LLM(
        llm_type,
        api_key,
        system,
        user,
        should_stop=should_stop,
        log=log
    )
    return resp


def merge_json(jsons: list) -> str:
    resp = None
    return resp


# 功能：从帖子的json文件提取报价数据记录
def json2data(gid: str, thread_json: str, should_stop=None, log=None) -> str:
    if thread_json is None:
        return -1
    if should_stop and should_stop():
        return "{}"

    env = config.get_env_cache()
    llm_type = env["LLM_TYPE"]
    api_key = env["API_KEY"]

    system = render_prompt(
        "2_extract/json2data_system.j2",
    )
    user = render_prompt(
        "2_extract/json2data_user.j2",
        thread_json=thread_json
    )

    _emit_log(log, f"正在处理json:{gid}")

    if should_stop and should_stop():
        return "{}"

    resp = call_LLM(
        llm_type,
        api_key,
        system,
        user,
        should_stop=should_stop,
        log=log
    )

    return resp







if __name__ == "__main__":
    config.refresh_env_cache()
    
    gid = "1855068947908617"
    in_path = Path("data/json/1_preprocessed") / f"{gid}.json"
    out_path = Path("data/json/2_extract") / f"{gid}.json"
    
    with open(in_path, "r", encoding="utf-8") as f:
        json_preprocess = f.read()
    
    json_extract = json2data(gid, json_preprocess)
    
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(json_extract)
    # with open(out_path, "w", encoding="utf-8") as f:
    #     json.dump(json_extract, f, ensure_ascii=False, indent=2)

    print("saved:", out_path)