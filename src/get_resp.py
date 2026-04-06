# 作者：郑凯峰
# 功能：发起Get请求并返回响应文本
# 输入：Url
# 输出：响应文本

import requests

def get_txt(url):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()  # 若code不是2XX，抛异常
    
    return resp.text

def get_json(url):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    
    return resp
