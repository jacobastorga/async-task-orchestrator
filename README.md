# Async Task Orchestrator & DAG Workflow Engine

[![Python](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![AsyncIO](https://img.shields.io/badge/concurrency-asyncio-green.svg)]()
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)]()

A high-performance asynchronous Directed Acyclic Graph (DAG) task orchestration framework with automated dependency resolution, cycle detection, concurrency controls, and execution persistence.

---

## Key Features

- **DAG Dependency Graphing:** Define complex task pipelines with arbitrary dependency topologies.
- **Topological Staging:** Uses Kahn's algorithm to resolve dependencies and group independent tasks into maximal parallel execution stages.
- **Cycle Detection:** Robust 3-color DFS graph traversal to catch circular dependencies before runtime.
- **Async Concurrency Throttling:** Non-blocking task execution managed via `asyncio.Semaphore` to prevent system exhaustion.
- **Exponential Backoff & Retries:** Built-in resilience for distributed network tasks and transient failures.
- **Embedded State Store:** SQLite execution history logging for auditing, durations, and outputs.

---

## Architecture

```
                       Task Graph Specification
                                  │
                                  ▼
                         ┌─────────────────┐
                         │     TaskDAG     │  (DFS Cycle Detection)
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │ TopologicalSort │  (Kahn's Staging)
                         └────────┬────────┘
                                  │
                     ┌────────────┴────────────┐
                     ▼                         ▼
              [Stage 1: A, B]           [Stage 2: C, D]
                     │                         │
                     └────────────┬────────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │  AsyncExecutor  │  (asyncio Concurrency Throttle)
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │   StateStore    │  (SQLite Run Telemetry)
                         └─────────────────┘
```

---

## Quickstart

### Installation
```bash
git clone https://github.com/jacobastorga/async-task-orchestrator.git
cd async-task-orchestrator
pip install -r requirements.txt
```

### Running Tests
```bash
pytest tests/ -v
```

### Example Usage
```python
import asyncio
from src.task import Task
from src.dag import TaskDAG
from src.executor import AsyncExecutor

async def main():
    dag = TaskDAG()
    dag.add_task(Task("extract", async_fetch_data))
    dag.add_task(Task("transform", process_records, dependencies={"extract"}))

    executor = AsyncExecutor(max_concurrency=4)
    result = await executor.run_dag(dag)
    print(result["transform"])

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Author
**Jacob Astorga**  
Software Engineer & Computational Scientist  
Portfolio: [github.com/jacobastorga](https://github.com/jacobastorga)
