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
import multiprocessing as mp
import logging
import traceback
import sys

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


class _QueuePrintWriter:
    def __init__(self, log_queue, event_type: str = "print", prefix: str = ""):
        self.log_queue = log_queue
        self.event_type = event_type
        self.prefix = prefix

    def write(self, message):
        if message is None:
            return
        text = str(message)
        if not text:
            return
        for line in text.splitlines():
            if line.strip():
                self.log_queue.put((self.event_type, f"{self.prefix}{line}"))

    def flush(self):
        return


class _QueueLogHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            message = self.format(record)
            self.log_queue.put(("log", message))
        except Exception:
            self.handleError(record)


def _get_gids_process_worker(rounds: int, result_queue, log_queue):
    queue_handler = _QueueLogHandler(log_queue)
    queue_handler.setLevel(logging.INFO)
    queue_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    ))

    root_logger = logging.getLogger()
    original_level = root_logger.level
    stdout_backup = sys.stdout
    stderr_backup = sys.stderr
    sys.stdout = _QueuePrintWriter(log_queue, event_type="print")
    sys.stderr = _QueuePrintWriter(log_queue, event_type="print", prefix="[stderr] ")
    root_logger.addHandler(queue_handler)
    if root_logger.level > logging.INFO:
        root_logger.setLevel(logging.INFO)

    try:
        log_queue.put(("log", "[TASK] get_gids 子进程已启动"))
        gids = get_gids(rounds)
        log_queue.put(("log", f"[TASK] get_gids 子进程执行完成，获取到 {len(gids)} 个 gid"))
        result_queue.put(("success", gids))
    except Exception as e:
        tb = traceback.format_exc()
        log_queue.put(("log", f"[TASK] get_gids 子进程异常: {e}"))
        log_queue.put(("log", tb.rstrip()))
        result_queue.put(("error", repr(e)))
    finally:
        sys.stdout = stdout_backup
        sys.stderr = stderr_backup
        root_logger.removeHandler(queue_handler)
        root_logger.setLevel(original_level)


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

    ctx = mp.get_context("spawn")
    gids_result_queue = ctx.Queue()
    gids_log_queue = ctx.Queue()
    gids_process = ctx.Process(
        target=_get_gids_process_worker,
        args=(rounds, gids_result_queue, gids_log_queue),
        daemon=True,
    )
    gids_process.start()

    while True:
        while True:
            try:
                event_type, event_payload = gids_log_queue.get_nowait()
                if event_type == "log":
                    _log(log, event_payload)
                elif event_type == "print":
                    print(event_payload)
            except Exception:
                break

        if should_stop and should_stop():
            if gids_process.is_alive():
                gids_process.terminate()
                gids_process.join(timeout=1)
            while True:
                try:
                    event_type, event_payload = gids_log_queue.get_nowait()
                    if event_type == "log":
                        _log(log, event_payload)
                    elif event_type == "print":
                        print(event_payload)
                except Exception:
                    break
            _progress(progress, {
                "stage": "finished",
                "message": "任务已停止",
                "total": 0,
                "done": 0,
                "success": 0,
                "empty": 0,
                "failed": 0,
                "current_gid": None,
            })
            _log(log, "[TASK] 在获取 gid 阶段收到停止信号，已强制终止 get_gids")
            return

        try:
            status, payload = gids_result_queue.get(timeout=0.2)
            gids_process.join(timeout=1)
            while True:
                try:
                    event_type, event_payload = gids_log_queue.get_nowait()
                    if event_type == "log":
                        _log(log, event_payload)
                    elif event_type == "print":
                        print(event_payload)
                except Exception:
                    break
            if status == "success":
                gids = payload
                break
            raise RuntimeError(f"get_gids 执行失败: {payload}")
        except Empty:
            if not gids_process.is_alive() and gids_result_queue.empty():
                gids_process.join(timeout=1)
                while True:
                    try:
                        event_type, event_payload = gids_log_queue.get_nowait()
                        if event_type == "log":
                            _log(log, event_payload)
                        elif event_type == "print":
                            print(event_payload)
                    except Exception:
                        break
                raise RuntimeError("get_gids 子进程已退出，但未返回结果")
            continue

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