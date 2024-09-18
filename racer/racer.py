from abc import ABC, abstractmethod
from queue import Queue
from threading import Thread
from typing import Callable, Dict, List, Tuple


class BaseTask(ABC):
    def __init__(
        self,
        name: str,
        target: Callable,
        args: Tuple = (),
        kwargs: Dict = {},
        use_prev_result: bool = False,
    ):
        self.name = name
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.use_prev_result = use_prev_result

    @abstractmethod
    def run(self, prev_result=None):
        pass

    def __str__(self):
        return f"{self.name}({self.args}, {self.kwargs})"


class Task(BaseTask):
    def __init__(
        self,
        name: str,
        target: Callable,
        args: Tuple = (),
        kwargs: Dict = {},
        use_prev_result: bool = False,
    ):
        super().__init__(name, target, args, kwargs, use_prev_result)

    def run(self, prev_result=None):
        if self.use_prev_result and prev_result is not None:
            if self.args:
                args = self.args + (prev_result,)
            else:
                args = (prev_result,)
            return self.target(*args, **self.kwargs)
        else:
            return self.target(*self.args, **self.kwargs)


class ParallelTask(BaseTask):
    def __init__(
        self,
        name: str,
        target: Callable,
        num_workers: int,
        args: Tuple = (),
        kwargs: Dict = {},
        use_prev_result: bool = False,
    ):
        super().__init__(name, target, args, kwargs, use_prev_result)
        self.num_workers = num_workers
        self.targets = [target] * num_workers

    def _worker(
        self,
        target: Callable,
        results: Queue,
        prev_result=None,
        *args,
        **kwargs,
    ):
        if self.use_prev_result and prev_result is not None:
            args = args + (prev_result,)
        result = target(*args, **kwargs)
        results.put(result)

    def run(self, prev_result=None):
        results = Queue()
        threads = [
            Thread(
                target=self._worker,
                args=(target, results, prev_result, *self.args),
                kwargs=self.kwargs,
            )
            for target in self.targets
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        result_list = []
        while not results.empty():
            result_list.append(results.get())

        return result_list


class Racer:
    def __init__(self, tasks: List[BaseTask]):
        self.tasks = tasks

    def _run_task_set(self, results: dict, thread_id: int):
        thread_results = {}
        prev_result = None
        for task in self.tasks:
            result = task.run(prev_result=prev_result)
            thread_results[f"{task.name}"] = result
            prev_result = result
        results[thread_id] = thread_results

    def run(self, num_clones: int = 1):
        threads = []
        results = {}

        for i in range(num_clones):
            thread = Thread(target=self._run_task_set, args=(results, i))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        return results
