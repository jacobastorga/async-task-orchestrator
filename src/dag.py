from typing import Dict, List, Set
from .task import Task

class CyclicDependencyError(ValueError):
    """Raised when the task graph contains one or more dependency cycles."""
    pass

class TaskDAG:
    """Directed Acyclic Graph (DAG) for managing task dependency hierarchies."""

    def __init__(self):
        self.tasks: Dict[str, Task] = {}

    def add_task(self, task: Task) -> None:
        """Registers a new task in the graph."""
        if task.name in self.tasks:
            raise ValueError(f"Task with name {task.name!r} already registered in DAG.")
        self.tasks[task.name] = task

    def validate(self) -> None:
        """Validates all dependencies exist and ensures no cycles exist via DFS 3-coloring."""
        for name, task in self.tasks.items():
            for dep in task.dependencies:
                if dep not in self.tasks:
                    raise KeyError(f"Task {name!r} depends on missing task {dep!r}.")

        # 0: Unvisited (White), 1: Visiting (Gray), 2: Visited (Black)
        visited = {name: 0 for name in self.tasks}

        def dfs(node: str, path: List[str]):
            visited[node] = 1
            path.append(node)
            for dep in self.tasks[node].dependencies:
                if visited[dep] == 1:
                    cycle = " -> ".join(path[path.index(dep):] + [dep])
                    raise CyclicDependencyError(f"Cycle detected in task graph: {cycle}")
                if visited[dep] == 0:
                    dfs(dep, path)
            visited[node] = 2
            path.pop()

        for name in self.tasks:
            if visited[name] == 0:
                dfs(name, [])

    def get_execution_stages(self) -> List[List[Task]]:
        """Groups tasks into parallelizable stages using Kahn's topological sort."""
        self.validate()
        in_degree = {name: len(task.dependencies) for name, task in self.tasks.items()}
        # Dependent adjacency list (dep -> tasks waiting on dep)
        dependents: Dict[str, List[str]] = {name: [] for name in self.tasks}
        for name, task in self.tasks.items():
            for dep in task.dependencies:
                dependents[dep].append(name)

        stages = []
        current_stage = [name for name, deg in in_degree.items() if deg == 0]

        while current_stage:
            stages.append([self.tasks[name] for name in current_stage])
            next_stage = []
            for name in current_stage:
                for dependent in dependents[name]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_stage.append(dependent)
            current_stage = next_stage

        return stages
