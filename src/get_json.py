# 作者：郑凯峰
# 功能：获取报价讨论帖的HTML内容，并保存到本地
# 输入：讨论帖URL id号
# 输出：HTML文件

from pathlib import Path
from get_data import fetch_rendered_html
from LLM import html2json, merge_json
import re
import json

from clean_html import html_to_clean_text, html_list_to_json_str

import config

# 文章url头(后面需加上id)
# PAGE_BASE_URL= "https://www.dongchedi.com/ugc/article/"


def gid2json(gid: str):
    print(gid)

    out_dir = Path("json/half")
    out_dir.mkdir(parents=True, exist_ok=True)

    p = out_dir / f"{gid}.json"
    print(f"目标路径存在？{p.exists()}")
    print(f"目标是文件？{p.is_file()}")
    # if p.exists() and p.is_file():
    #     return 1

    url = f"{config._ENV_CACHE['PAGE_BASE_URL']}{gid}"

    html_pages = fetch_rendered_html(url)
    json = html_list_to_json_str(html_pages)
    # json = html2json(gid, html_pages)

    with open(p, "w", encoding="utf-8") as f:
        f.write(json)

    print("saved:", p)

    return 0


def llm_gid2json(gid: str):
    """
    1) 拉取多页HTML（list[str]）
    2) 逐页调用 LLM(html2json) 抽取
    3) 调用 LLM(merge_json) 合并最终 JSON
    4) 保存到 json/half/{gid}.json
    """
    print(gid)

    out_dir = Path("json/half")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{gid}.json"
    print(f"目标路径存在？{out_path.exists()}")
    print(f"目标是文件？{out_path.is_file()}")

    base_url = f"{config._ENV_CACHE['PAGE_BASE_URL']}{gid}"

    # 1) 抓取多页渲染HTML（不落盘）
    html_pages = fetch_rendered_html(base_url)

    # 2) 先准备一个本地解析的兜底 JSON（便于对照/容灾）
    fallback_json_text = html_list_to_json_str(html_pages)

    # 3) 走 LLM：先逐页 html2json，再用 merge_json 合并
    try:
        page_json_texts = []
        for idx, html in enumerate(html_pages, start=1):
            # 逐页抽取：为了复用现有签名，这里按单页 list 传入
            one_page_json = html2json(gid, [html])
            s1 = (one_page_json or "").lstrip()
            if not (s1.startswith("{") or s1.startswith("[")):
                raise ValueError(f"LLM 单页输出不像 JSON（page={idx}，未以 {{ 或 [ 开头）")
            page_json_texts.append(one_page_json)

        llm_merged_text = merge_json(page_json_texts)
        s2 = (llm_merged_text or "").lstrip()
        if not (s2.startswith("{") or s2.startswith("[")):
            raise ValueError("LLM 合并输出不像 JSON（未以 { 或 [ 开头）")

        final_json_text = llm_merged_text
    except Exception as e:
        print(f"[warn] LLM 解析失败，改用本地clean_html兜底。原因：{e}")
        final_json_text = fallback_json_text

    # 4) 写文件
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(final_json_text)

    print("saved:", out_path)
    return 0


if __name__ == "__main__":
    config.get_env_cache()
    print(gid2json("1858457796005892"))