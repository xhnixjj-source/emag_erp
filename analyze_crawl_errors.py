#!/usr/bin/env python3
"""
分析 debug.log 和数据库 crawl_tasks 表的错误信息，找出爬取优化点
"""
import json
import sqlite3
import re
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# 配置路径
DEBUG_LOG_PATH = r"D:\emag_erp\.cursor\debug.log"
DB_PATH = r"\\10.147.19.69\emag_erp\backend\emag_erp.db"

def parse_debug_log(log_path: str) -> Dict:
    """解析 debug.log 文件，提取错误信息"""
    errors = {
        "task_failed": [],
        "page_goto_error": [],
        "category_page_goto_error": [],
        "store_page_goto_error": [],
        "intro_page_goto_error": [],
        "timeout_errors": [],
        "connection_errors": [],
        "window_restart": [],
        "window_cooldown": [],
    }
    
    print(f"正在读取日志文件: {log_path}")
    line_count = 0
    
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            for line in f:
                line_count += 1
                if not line.strip():
                    continue
                
                try:
                    log_entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                
                location = log_entry.get("location", "")
                message = log_entry.get("message", "")
                data = log_entry.get("data", {})
                timestamp = log_entry.get("timestamp", 0)
                
                # 任务失败
                if "task_failed" in location or message == "产品爬取任务最终失败":
                    errors["task_failed"].append({
                        "timestamp": timestamp,
                        "task_id": data.get("task_id"),
                        "url": data.get("product_url"),
                        "error_type": data.get("error_type"),
                        "error_message": data.get("error_message"),
                        "elapsed_sec": data.get("elapsed_sec"),
                    })
                
                # 页面加载错误
                if "page_goto_error" in location:
                    error_type = "product" if "product_data_crawler" in location else "other"
                    if "category" in location:
                        error_type = "category"
                    elif "store" in location:
                        error_type = "store"
                    elif "intro" in location:
                        error_type = "intro"
                    
                    error_info = {
                        "timestamp": timestamp,
                        "url": data.get("url") or data.get("page_url"),
                        "error_type": data.get("error_type"),
                        "error_message": data.get("error_message"),
                        "elapsed_ms": data.get("elapsed_ms"),
                        "attempt": data.get("attempt"),
                        "will_retry": data.get("will_retry"),
                    }
                    
                    if error_type == "product":
                        errors["page_goto_error"].append(error_info)
                    elif error_type == "category":
                        errors["category_page_goto_error"].append(error_info)
                    elif error_type == "store":
                        errors["store_page_goto_error"].append(error_info)
                    elif error_type == "intro":
                        errors["intro_page_goto_error"].append(error_info)
                
                # 超时错误
                if "Timeout" in str(data.get("error_message", "")):
                    errors["timeout_errors"].append({
                        "timestamp": timestamp,
                        "error_message": data.get("error_message"),
                        "url": data.get("url") or data.get("page_url"),
                    })
                
                # 连接错误
                error_msg = str(data.get("error_message", "")).lower()
                if "err_empty_response" in error_msg or "err_tunnel" in error_msg or "connection" in error_msg:
                    errors["connection_errors"].append({
                        "timestamp": timestamp,
                        "error_message": data.get("error_message"),
                        "url": data.get("url") or data.get("page_url"),
                        "elapsed_ms": data.get("elapsed_ms"),
                    })
                
                # 窗口重启
                if "窗口达到任务上限" in message or "主动重启" in message:
                    errors["window_restart"].append({
                        "timestamp": timestamp,
                        "window_id": data.get("window_id"),
                        "task_count": data.get("task_count"),
                    })
                
                # 窗口冷却
                if "窗口进入冷却期" in message or "cool_down" in str(data):
                    errors["window_cooldown"].append({
                        "timestamp": timestamp,
                        "window_id": data.get("window_id"),
                    })
    
    except FileNotFoundError:
        print(f"错误: 日志文件不存在: {log_path}")
        return errors
    except Exception as e:
        print(f"读取日志文件时出错: {e}")
        import traceback
        traceback.print_exc()
        return errors
    
    print(f"已读取 {line_count} 行日志")
    return errors

def query_database(db_path: str) -> Dict:
    """查询数据库中的失败任务"""
    results = {
        "failed_tasks": [],
        "error_summary": Counter(),
        "retry_summary": Counter(),
    }
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 查询失败的任务
        cursor.execute("""
            SELECT 
                id, task_type, product_url, status, retry_count, max_retries,
                error_message, created_at, updated_at, completed_at
            FROM crawl_tasks
            WHERE status = 'failed'
            ORDER BY updated_at DESC
            LIMIT 1000
        """)
        
        rows = cursor.fetchall()
        print(f"数据库中找到 {len(rows)} 个失败任务")
        
        for row in rows:
            task = {
                "id": row["id"],
                "task_type": row["task_type"],
                "product_url": row["product_url"],
                "status": row["status"],
                "retry_count": row["retry_count"],
                "max_retries": row["max_retries"],
                "error_message": row["error_message"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "completed_at": row["completed_at"],
            }
            results["failed_tasks"].append(task)
            
            # 分析错误消息
            error_msg = (row["error_message"] or "").lower()
            if "timeout" in error_msg:
                results["error_summary"]["timeout"] += 1
            elif "err_empty_response" in error_msg:
                results["error_summary"]["err_empty_response"] += 1
            elif "err_tunnel" in error_msg:
                results["error_summary"]["err_tunnel"] += 1
            elif "connection" in error_msg:
                results["error_summary"]["connection"] += 1
            elif "已获取到店铺介绍页url" in error_msg or "未获取到店铺商品列表url" in error_msg:
                results["error_summary"]["shop_url_extraction"] += 1
            else:
                results["error_summary"]["other"] += 1
            
            # 重试统计
            retry_count = row["retry_count"]
            results["retry_summary"][retry_count] += 1
        
        conn.close()
        
    except Exception as e:
        print(f"查询数据库时出错: {e}")
        import traceback
        traceback.print_exc()
    
    return results

def analyze_errors(log_errors: Dict, db_results: Dict) -> Dict:
    """分析错误模式，找出优化点"""
    analysis = {
        "summary": {},
        "optimization_suggestions": [],
    }
    
    # 1. 错误类型统计
    total_failed_tasks = len(log_errors["task_failed"])
    total_page_errors = len(log_errors["page_goto_error"])
    total_category_errors = len(log_errors["category_page_goto_error"])
    total_timeout = len(log_errors["timeout_errors"])
    total_connection = len(log_errors["connection_errors"])
    
    analysis["summary"] = {
        "total_failed_tasks": total_failed_tasks,
        "total_page_errors": total_page_errors,
        "total_category_errors": total_category_errors,
        "total_timeout_errors": total_timeout,
        "total_connection_errors": total_connection,
        "db_failed_tasks": len(db_results["failed_tasks"]),
        "db_error_summary": dict(db_results["error_summary"]),
        "retry_distribution": dict(db_results["retry_summary"]),
    }
    
    # 2. 分析 ERR_EMPTY_RESPONSE 的响应时间
    empty_response_times = [
        e["elapsed_ms"] for e in log_errors["connection_errors"]
        if "err_empty_response" in str(e.get("error_message", "")).lower()
        and e.get("elapsed_ms") is not None
    ]
    
    if empty_response_times:
        avg_time = sum(empty_response_times) / len(empty_response_times)
        min_time = min(empty_response_times)
        max_time = max(empty_response_times)
        analysis["summary"]["empty_response_stats"] = {
            "count": len(empty_response_times),
            "avg_ms": round(avg_time, 2),
            "min_ms": min_time,
            "max_ms": max_time,
        }
        
        # 如果平均响应时间 < 1000ms，说明是立即拒绝（IP被封）
        if avg_time < 1000:
            analysis["optimization_suggestions"].append({
                "priority": "HIGH",
                "issue": "ERR_EMPTY_RESPONSE 平均响应时间过短（< 1000ms）",
                "description": f"平均 {avg_time:.0f}ms，说明 emag.ro 立即拒绝连接，可能是代理 IP 被封",
                "suggestion": "1. 增加窗口冷却时间（BITBROWSER_TASK_COOLDOWN）\n2. 降低并发数\n3. 检查代理 IP 质量",
            })
    
    # 3. 分析重试成功率
    retry_success = sum(1 for e in log_errors["page_goto_error"] 
                       if isinstance(e.get("attempt"), int) and e.get("attempt", 0) > 1 
                       and e.get("will_retry") is False)
    retry_failed = sum(1 for e in log_errors["page_goto_error"] 
                      if isinstance(e.get("attempt"), int) and e.get("attempt", 0) > 1 
                      and e.get("will_retry") is True)
    
    if retry_success + retry_failed > 0:
        retry_success_rate = retry_success / (retry_success + retry_failed) * 100
        analysis["summary"]["retry_success_rate"] = round(retry_success_rate, 2)
        
        if retry_success_rate < 30:
            analysis["optimization_suggestions"].append({
                "priority": "MEDIUM",
                "issue": "重试成功率过低",
                "description": f"重试成功率仅 {retry_success_rate:.1f}%，说明大部分错误是永久性的（如 IP 被封）",
                "suggestion": "1. 减少重试次数，快速失败\n2. 增加窗口轮换频率\n3. 优化错误分类逻辑，区分临时错误和永久错误",
            })
    
    # 4. 分析超时错误
    timeout_30s = sum(1 for e in log_errors["timeout_errors"] 
                     if "30000" in str(e.get("error_message", "")))
    
    if timeout_30s > 0:
        analysis["optimization_suggestions"].append({
            "priority": "MEDIUM",
            "issue": "30秒超时错误较多",
            "description": f"发现 {timeout_30s} 个 30 秒超时错误",
            "suggestion": "1. 检查页面加载是否真的需要 30 秒\n2. 考虑增加超时时间到 45-60 秒\n3. 或者优化页面加载策略（减少等待的元素）",
        })
    
    # 5. 分析窗口重启和冷却
    restart_count = len(log_errors["window_restart"])
    cooldown_count = len(log_errors["window_cooldown"])
    
    if restart_count == 0:
        analysis["optimization_suggestions"].append({
            "priority": "LOW",
            "issue": "未发现窗口主动重启日志",
            "description": "新代码可能未生效，或者窗口还未达到重启阈值",
            "suggestion": "检查 BITBROWSER_MAX_TASKS_PER_WINDOW 配置和代码部署",
        })
    
    if cooldown_count == 0:
        analysis["optimization_suggestions"].append({
            "priority": "LOW",
            "issue": "未发现窗口冷却日志",
            "description": "新代码可能未生效，或者冷却逻辑未触发",
            "suggestion": "检查 BITBROWSER_TASK_COOLDOWN 配置和代码部署",
        })
    
    # 6. 分析店铺URL提取失败
    shop_url_errors = db_results["error_summary"].get("shop_url_extraction", 0)
    if shop_url_errors > 0:
        analysis["optimization_suggestions"].append({
            "priority": "MEDIUM",
            "issue": "店铺URL提取失败",
            "description": f"发现 {shop_url_errors} 个店铺URL提取失败的任务",
            "suggestion": "1. 检查店铺介绍页的 DOM 结构是否变化\n2. 增加重试机制\n3. 考虑使用备用提取方法",
        })
    
    # 7. 分析错误时间分布
    if log_errors["task_failed"]:
        timestamps = [e["timestamp"] for e in log_errors["task_failed"] if e.get("timestamp")]
        if timestamps:
            first_error = min(timestamps)
            last_error = max(timestamps)
            duration_minutes = (last_error - first_error) / 1000 / 60
            
            analysis["summary"]["error_time_span"] = {
                "first_error": datetime.fromtimestamp(first_error / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "last_error": datetime.fromtimestamp(last_error / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "duration_minutes": round(duration_minutes, 2),
            }
    
    # 8. 分析错误类型分布（从日志）
    error_type_counter = Counter()
    for task in log_errors["task_failed"]:
        error_type = task.get("error_type", "Unknown")
        error_type_counter[error_type] += 1
    
    analysis["summary"]["log_error_types"] = dict(error_type_counter)
    
    # 9. 分析 ERR_TUNNEL_CONNECTION_FAILED
    tunnel_errors = [
        e for e in log_errors["connection_errors"]
        if "err_tunnel" in str(e.get("error_message", "")).lower()
    ]
    if tunnel_errors:
        analysis["summary"]["tunnel_error_count"] = len(tunnel_errors)
        analysis["optimization_suggestions"].append({
            "priority": "HIGH",
            "issue": "ERR_TUNNEL_CONNECTION_FAILED 错误",
            "description": f"发现 {len(tunnel_errors)} 个隧道连接失败错误，说明代理连接不稳定",
            "suggestion": "1. 检查代理服务器状态\n2. 增加代理连接重试\n3. 考虑更换代理服务商",
        })
    
    return analysis

def print_report(analysis: Dict):
    """打印分析报告"""
    print("\n" + "="*80)
    print("爬取错误分析报告")
    print("="*80)
    
    # 摘要
    print("\n【错误统计摘要】")
    summary = analysis["summary"]
    print(f"  日志中的失败任务数: {summary.get('total_failed_tasks', 0)}")
    print(f"  数据库中的失败任务数: {summary.get('db_failed_tasks', 0)}")
    print(f"  产品页加载错误: {summary.get('total_page_errors', 0)}")
    print(f"  类目页加载错误: {summary.get('total_category_errors', 0)}")
    print(f"  超时错误: {summary.get('total_timeout_errors', 0)}")
    print(f"  连接错误: {summary.get('total_connection_errors', 0)}")
    
    if "empty_response_stats" in summary:
        stats = summary["empty_response_stats"]
        print(f"\n  ERR_EMPTY_RESPONSE 统计:")
        print(f"    数量: {stats['count']}")
        print(f"    平均响应时间: {stats['avg_ms']:.0f}ms")
        print(f"    最短: {stats['min_ms']:.0f}ms, 最长: {stats['max_ms']:.0f}ms")
    
    if "retry_success_rate" in summary:
        print(f"\n  重试成功率: {summary['retry_success_rate']:.1f}%")
    
    if "error_time_span" in summary:
        span = summary["error_time_span"]
        print(f"\n  错误时间跨度:")
        print(f"    首次错误: {span['first_error']}")
        print(f"    最后错误: {span['last_error']}")
        print(f"    持续时间: {span['duration_minutes']:.1f} 分钟")
    
    # 日志错误类型分布
    if summary.get("log_error_types"):
        print(f"\n【日志错误类型分布】")
        for error_type, count in summary["log_error_types"].items():
            print(f"  {error_type}: {count}")
    
    # 数据库错误分类
    if summary.get("db_error_summary"):
        print(f"\n【数据库错误分类】")
        for error_type, count in summary["db_error_summary"].items():
            print(f"  {error_type}: {count}")
    
    # 重试分布
    if summary.get("retry_distribution"):
        print(f"\n【重试次数分布】")
        for retry_count, task_count in sorted(summary["retry_distribution"].items()):
            print(f"  重试 {retry_count} 次: {task_count} 个任务")
    
    # 优化建议
    print("\n" + "="*80)
    print("【优化建议】")
    print("="*80)
    
    suggestions = analysis["optimization_suggestions"]
    if not suggestions:
        print("  未发现明显的优化点")
    else:
        for i, suggestion in enumerate(suggestions, 1):
            priority_emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(suggestion["priority"], "⚪")
            print(f"\n  {i}. {priority_emoji} [{suggestion['priority']}] {suggestion['issue']}")
            print(f"     问题: {suggestion['description']}")
            print(f"     建议:")
            for line in suggestion['suggestion'].split('\n'):
                print(f"       {line}")

def main():
    print("开始分析...")
    
    # 1. 解析日志
    log_errors = parse_debug_log(DEBUG_LOG_PATH)
    
    # 2. 查询数据库
    db_results = query_database(DB_PATH)
    
    # 3. 分析
    analysis = analyze_errors(log_errors, db_results)
    
    # 4. 打印报告
    print_report(analysis)
    
    print("\n" + "="*80)
    print("分析完成")
    print("="*80)

if __name__ == "__main__":
    main()