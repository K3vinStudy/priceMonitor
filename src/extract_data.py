# 功能：调用LLM来提取数据
# 输入：网站上提取到的原文本（非格式化）
# 输出：一条记录：时间、地区、汽车型号、价格（格式化）

import json
from pathlib import Path
import shutil

from LLM import json2data

import config

def json_to_data(gid: str, should_stop=None, log=None):
    if should_stop and should_stop():
        return None

    in_path = Path("data/json/1_preprocessed") / f"{gid}.json"
    out_path = Path("data/json/2_extracted") / f"{gid}.json"
    used_path = Path("data/json/3_used_pre") / f"{gid}.json"

    if used_path.exists():
        used_path.unlink()

    if should_stop and should_stop():
        return None

    with open(in_path, "r", encoding="utf-8") as f:
        json_preprocess = f.read()

    if should_stop and should_stop():
        return None

    data_json = json2data(gid, json_preprocess, should_stop=should_stop, log=log)

    if should_stop and should_stop():
        return None
    if not data_json or data_json == "{}":
        return None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(data_json)

    print("saved:", out_path)

    if should_stop and should_stop():
        return None

    used_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(in_path), str(used_path))

    return 0


def json2list(gid: str) -> list:
    json_path = Path("data/json/2_extracted") / f"{gid}.json"

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    top_series = data.get("series")
    fetched_at = data.get("fetched_at")
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
            "fetched_at": fetched_at,
            "evidence_where": evidence.get("where"),
            "evidence_content": evidence.get("content"),
        }
        # print(item)

        result.append(item)

    return result


def data2list(gid: str, should_stop=None, log=None) -> list:
    config.get_env_cache()

    if should_stop and should_stop():
        return []

    ret = json_to_data(gid, should_stop=should_stop, log=log)
    if ret is None:
        return []

    if should_stop and should_stop():
        return []

    result = json2list(gid)
    return result
    
    
    
    
if __name__ == "__main__":
    print(data2list(1848926554589193))