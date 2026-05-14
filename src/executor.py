import asyncio
import inspect
from typing import Dict, Any, Optional
from .dag import TaskDAG
from .task import Task, TaskStatus

class AsyncExecutor:
    """Asynchronous runtime executor managing task scheduling, concurrency, and retries."""

    def __init__(self, max_concurrency: int = 8):
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def _execute_single_task(self, task: Task, context: Dict[str, Any]) -> Any:
        async with self.semaphore:
            task.status = TaskStatus.RUNNING
            for attempt in range(task.retries + 1):
                task.attempts += 1
                try:
                    # Provide dependencies output to task if signature accepts context
                    sig = inspect.signature(task.fn)
                    kwargs = {}
                    if "context" in sig.parameters:
                        kwargs["context"] = context
                    for dep in task.dependencies:
                        if dep in sig.parameters and dep in context:
                            kwargs[dep] = context[dep]

                    if inspect.iscoroutinefunction(task.fn):
                        if task.timeout:
                            res = await asyncio.wait_for(task.fn(**kwargs), timeout=task.timeout)
                        else:
                            res = await task.fn(**kwargs)
                    else:
                        loop = asyncio.get_running_loop()
                        res = await loop.run_in_executor(None, lambda: task.fn(**kwargs))

                    task.result = res
                    task.status = TaskStatus.COMPLETED
                    return res

                except Exception as ex:
                    task.error = ex
                    if attempt < task.retries:
                        await asyncio.sleep(0.05 * (2 ** attempt))
                    else:
                        task.status = TaskStatus.FAILED
                        raise ex

    async def run_dag(self, dag: TaskDAG, initial_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Executes DAG stages sequentially, running tasks in each stage concurrently."""
        stages = dag.get_execution_stages()
        context: Dict[str, Any] = dict(initial_context) if initial_context else {}

        for stage_idx, stage_tasks in enumerate(stages):
            coros = [self._execute_single_task(t, context) for t in stage_tasks]
            results = await asyncio.gather(*coros, return_exceptions=True)

            for t, res in zip(stage_tasks, results):
                if isinstance(res, Exception):
                    raise RuntimeError(f"Workflow failed at stage {stage_idx} in task {t.name!r}: {res}")
                context[t.name] = res

        return context
