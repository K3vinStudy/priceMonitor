# 作者：郑凯峰
# 功能：读取环境变量等，生成全局变量

from pathlib import Path
from dotenv import load_dotenv
import os

_ENV_CACHE = None

# 从环境中载入对应的API_Key
def load_api_key(LLM):
    load_dotenv()
    match LLM:
        case 1:
            api_key = os.getenv('Qwen_API_KEY')
        case 2:
            api_key = os.getenv('GPT_API_KEY')
        case _:
            raise Exception("Unsupported LLM. Please check LLM.py.")
        
    if not api_key:
        print("警告：未找到 API KEY，请检查 /.env 文件")
        # 需要在工作目录建立.env文件，并输入    MY_API_KEY=xxxxxxxxxxxxxxxx
    else:
        print("成功读取到 API KEY")
    
    return api_key
        

def load_env():
    # 选择LLM:
    #       1.千问
    #       2.GPT
    llm = 1
    api_key = load_api_key(llm)
    data_dir = Path.cwd() / "data"
    base_url = "https://www.dongchedi.com/motor/pc/ugc/community/topic_list?aid=1839&app_name=auto_web_pc&topic_id=92642&type=all&limit=30&sort_type=2&last_id="
    first_url = "https://www.dongchedi.com/motor/pc/ugc/community/topic_list?aid=1839&app_name=auto_web_pc&topic_id=92642&type=all&limit=1&sort_type=2"
    page_base_url = "https://www.dongchedi.com/ugc/article/"

    return {
        "LLM_TYPE": llm,
        "API_KEY": api_key,
        "DATA_DIR": data_dir,
        "BASE_URL": base_url,
        "FIRST_URL": first_url,
        "PAGE_BASE_URL": page_base_url
    }


# 生成环境变量全局缓存
def get_env_cache():
    global _ENV_CACHE
    if _ENV_CACHE is None:
        _ENV_CACHE = load_env()
    return _ENV_CACHE