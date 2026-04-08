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

from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
import threading

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


def _safe_qsize(q):
    try:
        return q.qsize()
    except NotImplementedError:
        return -1

def setup_app(log=None):
    config.get_env_cache()
    init_db()
    _log(log, "数据库已就绪")
    

def run_data2list_with_retry(gid, max_retries=1, log=None, should_stop=None):
    for attempt in range(max_retries + 1):
        if should_stop and should_stop():
            _log(log, f"[LLM] gid={gid} 收到停止信号，终止处理")
            return "stopped", []

        try:
            records = data2list(gid, should_stop, log)
            if records:
                return "success", records
            return "empty", []
        except Exception as e:
            _log(log, f"[LLM] gid={gid} 异常，attempt={attempt + 1}，error={e}")

        if should_stop and should_stop():
            _log(log, f"[LLM] gid={gid} 收到停止信号，终止重试")
            return "stopped", []

        if attempt < max_retries:
            _log(log, f"[LLM] gid={gid} 准备重试")

    _log(log, f"[LLM] gid={gid} 最终失败，放弃处理")
    return "failed", []

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

    gid_queue = Queue(maxsize=max(llm_workers * 2, 10))
    result_queue = Queue(maxsize=max(llm_workers * 2, 10))
    db_buffer = []
    stopped = False
    llm_stop_token = object()
    result_stop_token = object()
    producer_done = threading.Event()
    workers_done = threading.Event()

    def update_progress():
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

    def producer():
        nonlocal done, success, stopped
        try:
            for gid in gids:
                if should_stop and should_stop():
                    stopped = True
                    _log(log, "[TASK] 收到停止信号，停止提交新任务")
                    break

                if gid_exists(gid):
                    done += 1
                    success += 1
                    _log(log, f"[TASK] gid={gid} 已存在，跳过")
                    update_progress()
                    continue

                gid2json(gid)

                while True:
                    if should_stop and should_stop():
                        stopped = True
                        _log(log, "[TASK] 收到停止信号，停止提交新任务")
                        return
                    try:
                        gid_queue.put(gid, timeout=0.5)
                        break
                    except Exception:
                        continue
        finally:
            producer_done.set()
            for _ in range(llm_workers):
                gid_queue.put(llm_stop_token)
            _log(log, "[TASK] gid 生产完成，已发送 LLM 停止信号")

    def llm_worker(worker_id: int):
        while True:
            if should_stop and should_stop():
                break

            gid = gid_queue.get()
            try:
                if gid is llm_stop_token:
                    break

                status, records = run_data2list_with_retry(gid, 1, log, should_stop)
                result_queue.put((gid, status, records))
            finally:
                gid_queue.task_done()

    def worker_manager():
        threads = []
        for worker_id in range(llm_workers):
            t = threading.Thread(target=llm_worker, args=(worker_id + 1,), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        workers_done.set()
        result_queue.put(result_stop_token)
        _log(log, "[TASK] 所有 LLM worker 已结束，已发送结果停止信号")

    producer_thread = threading.Thread(target=producer, daemon=True)
    manager_thread = threading.Thread(target=worker_manager, daemon=True)
    producer_thread.start()
    manager_thread.start()

    while True:
        if should_stop and should_stop() and not stopped:
            stopped = True
            _log(log, "[TASK] 收到停止信号，停止处理剩余结果")

        item = result_queue.get()
        try:
            if item is result_stop_token:
                break

            gid, status, records = item
            done += 1

            if status == "failed":
                failed += 1
                _log(log, f"[LLM] gid={gid} 处理失败，跳过入库")
                update_progress()
                continue

            if status == "stopped":
                failed += 1
                _log(log, f"[LLM] gid={gid} 因停止信号结束，跳过入库")
                update_progress()
                continue

            if status == "empty":
                success += 1
                empty += 1
                _log(log, f"[LLM] gid={gid} 正常返回空列表，跳过入库")
                update_progress()
                continue

            success += 1
            db_buffer.extend(records)
            _log(log, f"[DB] 新增 {len(records)} 条，当前等待入库 {len(db_buffer)} 条")

            if len(db_buffer) >= db_batch_size:
                insert_price_records(db_buffer)
                _log(log, f"[DB] 已批量插入 {len(db_buffer)} 条")
                db_buffer.clear()

            update_progress()
        finally:
            result_queue.task_done()

    producer_thread.join()
    manager_thread.join()

    if db_buffer:
        insert_price_records(db_buffer)
        _log(log, f"[DB] 已收尾插入 {len(db_buffer)} 条")
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