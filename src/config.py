# 功能：读取环境变量等，生成全局变量

from pathlib import Path
from dotenv import load_dotenv
import os

_ENV_CACHE = None
ENV_FILE = Path.cwd() / ".env"

DEFAULT_ENV = {
    "LLM": "1",
    "MODEL_QWEN": "qwen3.5-plus",
    "MODEL_GPT": "gpt-5.2",
    "Qwen_API_KEY": "",
    "GPT_API_KEY": "",
    "DATA_DIR": "data",
    # "FETCH_ROUND": "1",
    "BASE_URL": "",
    "FIRST_URL": "",
    "PAGE_BASE_URL": "",
    "LLM_WORKERS": "15",
    "LLM_TIMEOUT_SECONDS": "600",
    "DB_BATCH_SIZE": "50",
}


def write_env_file(env_dict: dict):
    lines = [f"{key}={value}" for key, value in env_dict.items()]
    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def ensure_env_file():
    if not ENV_FILE.exists():
        write_env_file(DEFAULT_ENV)


def reset_env_file():
    write_env_file(DEFAULT_ENV)


def refresh_env_cache():
    global _ENV_CACHE
    _ENV_CACHE = load_env()
    return _ENV_CACHE

# 从环境中载入对应的API_Key
def load_api_key(LLM):
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


def load_model_type(LLM):
    match LLM:
        case 1:
            model_type = os.getenv('MODEL_QWEN')
        case 2:
            model_type = os.getenv('MODEL_GPT')
        case _:
            raise Exception("Unsupported LLM. Please check LLM.py.")
    
    return model_type
        

def load_env():
    ensure_env_file()
    load_dotenv(dotenv_path=ENV_FILE, override=True)
    
    llm = int(os.getenv('LLM', DEFAULT_ENV['LLM']))
    model_type = load_model_type(llm)
    api_key = load_api_key(llm)
    data_dir = Path.cwd() / os.getenv('DATA_DIR', DEFAULT_ENV['DATA_DIR'])
    # fetch_round = os.getenv('FETCH_ROUND', DEFAULT_ENV['FETCH_ROUND'])
    base_url = os.getenv('BASE_URL', DEFAULT_ENV['BASE_URL'])
    first_url = os.getenv('FIRST_URL', DEFAULT_ENV['FIRST_URL'])
    page_base_url = os.getenv('PAGE_BASE_URL', DEFAULT_ENV['PAGE_BASE_URL'])

    return {
        "LLM_TYPE": llm,
        "MODEL_TYPE": model_type,
        "API_KEY": api_key,
        "DATA_DIR": data_dir,
        # "FETCH_ROUND": fetch_round,
        "BASE_URL": base_url,
        "FIRST_URL": first_url,
        "PAGE_BASE_URL": page_base_url,
        "LLM_WORKERS": int(os.getenv('LLM_WORKERS', DEFAULT_ENV['LLM_WORKERS'])),
        "LLM_TIMEOUT_SECONDS": int(os.getenv('LLM_TIMEOUT_SECONDS', DEFAULT_ENV['LLM_TIMEOUT_SECONDS'])),
        "DB_BATCH_SIZE": int(os.getenv('DB_BATCH_SIZE', DEFAULT_ENV['DB_BATCH_SIZE'])),
    }


# 获取环境变量全局缓存；首次调用时自动加载
def get_env_cache():
    global _ENV_CACHE
    if _ENV_CACHE is None:
        return refresh_env_cache()
    return _ENV_CACHE