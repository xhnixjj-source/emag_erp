"""手动触发监控：异步任务状态（内存存储，进程重启后丢失）"""
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

_jobs_lock = threading.Lock()
_jobs: Dict[str, Dict[str, Any]] = {}


def create_job(user_id: int) -> str:
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "user_id": user_id,
            "status": "running",
            "total": 0,
            "processed": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "message": "",
            "started_at": datetime.utcnow().isoformat() + "Z",
            "finished_at": None,
        }
    return job_id


def update_job(job_id: str, **fields: Any) -> None:
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id].update(fields)


def finalize_job(
    job_id: str,
    status: str,
    message: str,
    *,
    processed: Optional[int] = None,
    success: Optional[int] = None,
    failed: Optional[int] = None,
    skipped: Optional[int] = None,
) -> None:
    finished = datetime.utcnow().isoformat() + "Z"
    with _jobs_lock:
        if job_id not in _jobs:
            return
        j = _jobs[job_id]
        j["status"] = status
        j["message"] = message
        j["finished_at"] = finished
        if processed is not None:
            j["processed"] = processed
        if success is not None:
            j["success"] = success
        if failed is not None:
            j["failed"] = failed
        if skipped is not None:
            j["skipped"] = skipped


def get_job(job_id: str, user_id: int) -> Optional[Dict[str, Any]]:
    with _jobs_lock:
        j = _jobs.get(job_id)
        if not j:
            return None
        if j.get("user_id") != user_id:
            return None
        return dict(j)


def get_job_internal(job_id: str) -> Optional[Dict[str, Any]]:
    """服务端内部使用（如写操作日志），不校验 user_id"""
    with _jobs_lock:
        j = _jobs.get(job_id)
        return dict(j) if j else None
