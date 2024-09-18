from typing import List

from racer import BaseTask, ParallelTask, Racer, Task


def add(x: int, y: int):
    return x + y


def sub(x: int, y: int, z: int):
    return x - y - z


def mul(x: int, y: int, z: int):
    return x * y * z


if __name__ == "__main__":
    task1 = Task(name="task1", target=add, kwargs={"x": 1, "y": 5})
    task2 = Task(name="task2", target=sub, args=(3, 4), use_prev_result=True)
    task3 = ParallelTask(
        name="task3",
        target=mul,
        num_workers=3,
        args=(5, 6),
        use_prev_result=True,
    )

    # tasks will be run sequentially
    tasks: List[BaseTask] = [task1, task2, task3]
    racer = Racer(tasks)
    results = racer.run(2)
    print(results)
