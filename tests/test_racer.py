import unittest
from typing import List

from racer import BaseTask, ParallelTask, Racer, Task


def sample_task(x, y):
    return x + y


def sample_task_with_prev_result(x, y, prev_result):
    return x + y + prev_result


class TestTask(unittest.TestCase):
    def test_task_run(self):
        task = Task(name="task1", target=sample_task, args=(1, 2))
        result = task.run()
        self.assertEqual(result, 3)

    def test_task_run_with_prev_result(self):
        task = Task(
            name="task2",
            target=sample_task_with_prev_result,
            args=(1, 2),
            use_prev_result=True,
        )
        result = task.run(prev_result=3)
        self.assertEqual(result, 6)


class TestParallelTask(unittest.TestCase):
    def test_parallel_task_run(self):
        task = ParallelTask(
            name="parallel_task1", target=sample_task, num_workers=3, args=(1, 2)
        )
        results = task.run()
        self.assertEqual(results, [3, 3, 3])

    def test_parallel_task_run_with_prev_result(self):
        task = ParallelTask(
            name="parallel_task2",
            target=sample_task_with_prev_result,
            num_workers=3,
            args=(1, 2),
            use_prev_result=True,
        )
        results = task.run(prev_result=3)
        self.assertEqual(results, [6, 6, 6])


class TestRacer(unittest.TestCase):
    def test_racer_run(self):
        tasks: List[BaseTask] = [
            Task(
                name="task1",
                target=sample_task,
                args=(1, 2),
            ),
            Task(
                name="task2",
                target=sample_task_with_prev_result,
                args=(3, 4),
                use_prev_result=True,
            ),
            ParallelTask(
                name="task3",
                target=sample_task_with_prev_result,
                num_workers=2,
                args=(5, 6),
                use_prev_result=True,
            ),
        ]
        racer = Racer(tasks=tasks)
        results = racer.run(num_clones=2)
        expected_results = {
            0: {"task1": 3, "task2": 10, "task3": [21, 21]},
            1: {"task1": 3, "task2": 10, "task3": [21, 21]},
        }
        self.assertEqual(results, expected_results)


if __name__ == "__main__":
    unittest.main()
