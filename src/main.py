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

from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed

from extract_data import data2list
from get_data import get_gids
from get_json import gid2json
import config


def _log(log, message: str):
    if log is not None:
        log(message)
    else:
        print(message)



def _progress(progress, payload: dict):
    if progress is not None:
        progress(payload)

def setup_app(log=None):
    config.get_env_cache()
    init_db()
    _log(log, "数据库已就绪")
    

def run_data2list_with_retry(gid, max_retries=1, log=None, should_stop=None):
    env = config.get_env_cache()
    llm_timeout_seconds = env['LLM_TIMEOUT_SECONDS']

    for attempt in range(max_retries + 1):
        if should_stop and should_stop():
            _log(log, f"[LLM] gid={gid} 收到停止信号，终止处理")
            return []

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(data2list, gid, should_stop)
            try:
                records = future.result(timeout=llm_timeout_seconds)
                return records
            except TimeoutError:
                _log(log, f"[LLM] gid={gid} 超时（{llm_timeout_seconds}s），attempt={attempt + 1}")
            except Exception as e:
                _log(log, f"[LLM] gid={gid} 异常，attempt={attempt + 1}，error={e}")

        if should_stop and should_stop():
            _log(log, f"[LLM] gid={gid} 收到停止信号，终止重试")
            return []

        if attempt < max_retries:
            _log(log, f"[LLM] gid={gid} 准备重试")

    _log(log, f"[LLM] gid={gid} 最终失败，放弃处理")
    return []

def fetch(rounds: int, log=None, progress=None, should_stop=None):
    env = config.get_env_cache()
    llm_workers = env['LLM_WORKERS']
    db_batch_size = env['DB_BATCH_SIZE']

    _progress(progress, {
        "stage": "getting_gids",
        "message": "正在批量获取gid...",
    })
    _log(log, "[TASK] 正在批量获取gid...")

    gids = get_gids(rounds)
    total = len(gids)
    done = 0
    success = 0
    empty = 0
    failed = 0

    _progress(progress, {
        "stage": "processing_init",
        "message": f"已获取 {total} 个gid，开始处理",
        "total": total,
        "done": 0,
        "success": 0,
        "empty": 0,
        "failed": 0,
        "current_gid": None,
    })
    _log(log, f"[TASK] 已获取 {total} 个gid，开始处理")

    db_buffer = []
    future_to_gid = {}
    stopped = False

    with ThreadPoolExecutor(max_workers=llm_workers) as executor:
        for gid in gids:
            if should_stop and should_stop():
                stopped = True
                _log(log, "[TASK] 收到停止信号，停止提交新任务")
                break

            if gid_exists(gid):
                done += 1
                success += 1
                _log(log, f"[TASK] gid={gid} 已存在，跳过")
                _progress(progress, {
                    "stage": "processing",
                    "message": f"正在处理 gid：{done}/{total}",
                    "total": total,
                    "done": done,
                    "success": success,
                    "empty": empty,
                    "failed": failed,
                    "current_gid": None,
                })
                continue

            gid2json(gid)
            future = executor.submit(run_data2list_with_retry, gid, 1, log, should_stop)
            future_to_gid[future] = gid

        for future in as_completed(future_to_gid):
            gid = future_to_gid[future]

            if should_stop and should_stop():
                stopped = True
                _log(log, "[TASK] 收到停止信号，停止处理剩余结果")
                for pending_future in future_to_gid:
                    if not pending_future.done():
                        pending_future.cancel()
                break

            try:
                records = future.result()
            except Exception as e:
                _log(log, f"[LLM] gid={gid} 主线程取结果失败，error={e}")
                failed += 1
                done += 1
                _progress(progress, {
                    "stage": "processing",
                    "message": f"正在处理 gid：{done}/{total}",
                    "total": total,
                    "done": done,
                    "success": success,
                    "empty": empty,
                    "failed": failed,
                    "current_gid": None,
                })
                continue

            done += 1

            if not records:
                success += 1
                empty += 1
                _log(log, f"[LLM] gid={gid} 正常返回空列表，跳过入库")
                _progress(progress, {
                    "stage": "processing",
                    "message": f"正在处理 gid：{done}/{total}",
                    "total": total,
                    "done": done,
                    "success": success,
                    "empty": empty,
                    "failed": failed,
                    "current_gid": None,
                })
                continue

            success += 1
            db_buffer.extend(records)
            _log(log, f"[DB] gid={gid} 累积待入库 {len(db_buffer)} 条")

            if len(db_buffer) >= db_batch_size:
                insert_price_records(db_buffer)
                _log(log, f"[DB] 批量插入 {len(db_buffer)} 条")
                db_buffer.clear()

            _progress(progress, {
                "stage": "processing",
                "message": f"正在处理 gid：{done}/{total}",
                "total": total,
                "done": done,
                "success": success,
                "empty": empty,
                "failed": failed,
                "current_gid": None,
            })

    if db_buffer:
        insert_price_records(db_buffer)
        _log(log, f"[DB] 收尾插入 {len(db_buffer)} 条")
        db_buffer.clear()

    final_message = "任务已停止" if stopped else "处理完成"
    _progress(progress, {
        "stage": "finished",
        "message": final_message,
        "total": total,
        "done": done,
        "success": success,
        "empty": empty,
        "failed": failed,
        "current_gid": None,
    })
    _log(log, f"[TASK] {final_message}")
        
        
        
        
    
if __name__ == "__main__":
    setup_app()
    fetch(1)