from abc import ABC, abstractmethod
from queue import Queue
from threading import Thread
from typing import Any, Callable, Dict, List, Optional, Tuple


class BaseTask(ABC):
    def __init__(
        self,
        name: str,
        target: Callable,
        args: Tuple = (),
        kwargs: Dict = {},
        use_prev_result: bool = False,  # Flag to indicate if this task wants the previous result
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
        # Pass previous result if needed
        if self.use_prev_result and prev_result is not None:
            self.args = self.args + (prev_result,)
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
        self.results = Queue()  # Queue to store results from each thread

    def worker(self, target: Callable, *args, **kwargs):
        result = target(*args, **kwargs)
        self.results.put(result)

    def run(self, prev_result=None):
        # Pass previous result if needed
        if self.use_prev_result and prev_result is not None:
            self.args = self.args + (prev_result,)

        threads = [
            Thread(target=self.worker, args=(target, *self.args), kwargs=self.kwargs)
            for target in self.targets
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Collect results from the queue
        result_list = []
        while not self.results.empty():
            result_list.append(self.results.get())

        return result_list


class Racer:
    def __init__(self, tasks: List[BaseTask]):
        self.tasks = tasks

    def run(self):
        results = {}
        prev_result = None
        for task in self.tasks:
            result = task.run(prev_result=prev_result)
            results[task.name] = result
            prev_result = result  # Pass the current result to the next task
        return results
