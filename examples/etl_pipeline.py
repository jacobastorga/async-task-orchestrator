import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.task import Task
from src.dag import TaskDAG
from src.executor import AsyncExecutor

async def extract_users():
    await asyncio.sleep(0.02)
    return [{"id": 1, "name": "Alice", "role": "engineer"}, {"id": 2, "name": "Bob", "role": "analyst"}]

async def extract_logs():
    await asyncio.sleep(0.03)
    return [{"user_id": 1, "action": "login"}, {"user_id": 2, "action": "query"}]

def join_data(extract_users, extract_logs):
    combined = []
    log_map = {l["user_id"]: l["action"] for l in extract_logs}
    for u in extract_users:
        combined.append({**u, "last_action": log_map.get(u["id"], "none")})
    return combined

def compute_metrics(join_data):
    return {"total_records": len(join_data), "active_users": [r["name"] for r in join_data]}

async def main():
    dag = TaskDAG()
    dag.add_task(Task("extract_users", extract_users))
    dag.add_task(Task("extract_logs", extract_logs))
    dag.add_task(Task("join_data", join_data, dependencies={"extract_users", "extract_logs"}))
    dag.add_task(Task("compute_metrics", compute_metrics, dependencies={"join_data"}))

    executor = AsyncExecutor(max_concurrency=4)
    print("Executing Async DAG Pipeline...")
    res = await executor.run_dag(dag)
    print("Pipeline Execution Completed!")
    print(f"Metrics Output: {res['compute_metrics']}")

if __name__ == "__main__":
    asyncio.run(main())
