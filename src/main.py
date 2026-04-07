from database.init_db import init_db
from database.op import (
    insert_price_record,
    insert_price_records,
    get_price_record_by_ruid,
    list_price_records,
    query_price_records,
    gid_exists,
    delete_price_record_by_ruid,
    delete_price_records_by_gid,
    count_price_records,
    count_price_records_by_gid,
)
init_db()
print("数据库已就绪")

from extract_data import data2list
from get_data import get_gids
from get_json import gid2json
import config
from concurrent.futures import ThreadPoolExecutor, TimeoutError

LLM_WORKERS = 8
LLM_TIMEOUT_SECONDS = 600
DB_BATCH_SIZE = 20


def run_data2list_with_retry(gid, max_retries=1):
    for attempt in range(max_retries + 1):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(data2list, gid)
            try:
                records = future.result(timeout=LLM_TIMEOUT_SECONDS)
                return records
            except TimeoutError:
                print(f"[LLM] gid={gid} 超时（{LLM_TIMEOUT_SECONDS}s），attempt={attempt + 1}")
            except Exception as e:
                print(f"[LLM] gid={gid} 异常，attempt={attempt + 1}，error={e}")

        if attempt < max_retries:
            print(f"[LLM] gid={gid} 准备重试")

    print(f"[LLM] gid={gid} 最终失败，放弃处理")
    return []

def fetch(rounds:int):
    config.get_env_cache()
    gids = set()
    gids = get_gids(rounds)

    db_buffer = []
    future_to_gid = {}

    with ThreadPoolExecutor(max_workers=LLM_WORKERS) as executor:
        for gid in gids:
            if gid_exists(gid):
                continue

            gid2json(gid)
            future = executor.submit(run_data2list_with_retry, gid, 1)
            future_to_gid[future] = gid

        for future, gid in future_to_gid.items():
            try:
                records = future.result()
            except Exception as e:
                print(f"[LLM] gid={gid} 主线程取结果失败，error={e}")
                records = []

            if not records:
                print(f"[LLM] gid={gid} 正常返回空列表，跳过入库")
                continue

            db_buffer.extend(records)
            print(f"[DB] gid={gid} 累积待入库 {len(db_buffer)} 条")

            if len(db_buffer) >= DB_BATCH_SIZE:
                insert_price_records(db_buffer)
                print(f"[DB] 批量插入 {len(db_buffer)} 条")
                db_buffer.clear()

    if db_buffer:
        insert_price_records(db_buffer)
        print(f"[DB] 收尾插入 {len(db_buffer)} 条")
        db_buffer.clear()
        

        
        
        
    
    
if __name__ == "__main__":
    fetch(1)