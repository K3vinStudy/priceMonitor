# 功能：调用LLM来提取数据
# 输入：网站上提取到的原文本（非格式化）
# 输出：一条记录：时间、地区、汽车型号、价格（格式化）

import json
from pathlib import Path
import shutil

from LLM import json2data

import config

def json_to_data(gid: str):
    in_path = Path("data/json/1_preprocess") / f"{gid}.json"
    out_path = Path("data/json/2_extract") / f"{gid}.json"
    used_path = Path("data/json/3_used_pre") / f"{gid}.json"
    
    if(out_path.exists() and used_path.exists()):
        in_txt = in_path.read_text(encoding="utf-8")
        used_txt = used_path.read_text(encoding="utf-8")
        
        print(f"gid {gid} 已有数据无需更新：{in_txt == used_txt}")    # 是否相等
        if(in_txt == used_txt):
            in_path.unlink()
            return 1
        else:
            used_path.unlink()
    
    with open(in_path, "r", encoding="utf-8") as f:
        json_preprocess = f.read()
    
    data_json = json2data(gid, json_preprocess)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(data_json)

    print("saved:", out_path)
    
    used_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(in_path), str(used_path))
    
    return 0


def json2list(gid: str) -> list:
    json_path = Path("data/json/2_extract") / f"{gid}.json"

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    top_series = data.get("series")
    top_source_url = data.get("source_url")
    records = data.get("records", [])

    result = []

    for record in records:
        evidence = record.get("evidence") or {}

        item = {
            "series": record.get("series") or top_series,
            "price_cny": float(record.get("price_cny")),
            "date": record.get("date"),
            "location": record.get("location") or record.get("location_raw"),
            "source_url": record.get("source_url") or top_source_url,
            "gid": gid,
            "evidence_where": evidence.get("where"),
            "evidence_content": evidence.get("content"),
        }
        print(item)

        result.append(item)

    return result


def data2list(gid: str) -> list:
    config.get_env_cache()
    
    json_to_data(gid)
    
    list = json2list(gid)
    return list
    
    
    
    
    
    
if __name__ == "__main__":
    print(data2list(1848926554589193))