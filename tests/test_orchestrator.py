import unittest
import asyncio
from src.task import Task, TaskStatus
from src.dag import TaskDAG, CyclicDependencyError
from src.executor import AsyncExecutor
from src.state_store import SQLiteExecutionStore

class TestAsyncOrchestrator(unittest.TestCase):

    def setUp(self):
        self.executor = AsyncExecutor(max_concurrency=4)

    def test_dag_cycle_detection(self):
        dag = TaskDAG()
        dag.add_task(Task("A", lambda: 1, dependencies={"B"}))
        dag.add_task(Task("B", lambda: 2, dependencies={"A"}))
        with self.assertRaises(CyclicDependencyError):
            dag.validate()

    def test_linear_dag_execution(self):
        async def run_test():
            dag = TaskDAG()
            dag.add_task(Task("load_num", lambda: 10))
            dag.add_task(Task("multiply", lambda load_num: load_num * 5, dependencies={"load_num"}))
            dag.add_task(Task("add_offset", lambda multiply: multiply + 3, dependencies={"multiply"}))

            context = await self.executor.run_dag(dag)
            self.assertEqual(context["load_num"], 10)
            self.assertEqual(context["multiply"], 50)
            self.assertEqual(context["add_offset"], 53)

        asyncio.run(run_test())

    def test_parallel_stages(self):
        async def run_test():
            dag = TaskDAG()
            dag.add_task(Task("source", lambda: "base"))
            dag.add_task(Task("branch_1", lambda source: f"{source}_left", dependencies={"source"}))
            dag.add_task(Task("branch_2", lambda source: f"{source}_right", dependencies={"source"}))
            dag.add_task(Task("merge", lambda branch_1, branch_2: f"{branch_1}+{branch_2}", dependencies={"branch_1", "branch_2"}))

            context = await self.executor.run_dag(dag)
            self.assertEqual(context["merge"], "base_left+base_right")

        asyncio.run(run_test())

    def test_retry_mechanism(self):
        async def run_test():
            calls = {"count": 0}
            def flaky_service():
                calls["count"] += 1
                if calls["count"] < 2:
                    raise ConnectionResetError("Transient network failure")
                return "recovered"

            dag = TaskDAG()
            dag.add_task(Task("flaky", flaky_service, retries=2))
            context = await self.executor.run_dag(dag)
            self.assertEqual(context["flaky"], "recovered")
            self.assertEqual(calls["count"], 2)

        asyncio.run(run_test())

    def test_sqlite_persistence(self):
        store = SQLiteExecutionStore(":memory:")
        t = Task("sample_task", lambda: 42)
        t.status = TaskStatus.COMPLETED
        t.result = 42
        store.record_run("run-001", "SUCCESS", 100.0, 102.5, [t])
        summary = store.get_run_summary("run-001")
        self.assertIsNotNone(summary)
        self.assertEqual(summary["duration_seconds"], 2.5)
        self.assertEqual(summary["status"], "SUCCESS")

if __name__ == "__main__":
    unittest.main()
