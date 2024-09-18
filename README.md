# Racer

Racer is a simple Python task runner framework that supports sequential and parallel task execution. It also allows the result of one task to be passed to the next, making it flexible for various workflows.

## Features

- **Task Execution**: Run tasks sequentially or in parallel.
- **Task Result Propagation**: Optionally pass the result of a task to the next task in the queue.
- **Multithreading Support**: Execute tasks in parallel using multiple threads.
- **Customizable Tasks**: Easily define custom tasks by extending the `BaseTask` class.


## Getting Started

### Basic Usage
Define your tasks using either the `Task` or `ParallelTask` class, then use the `Racer` class to run them sequentially.

```python
from racer import Task, ParallelTask, Racer

def add(x: int, y: int):
    return x + y

def mul(x: int, y: int):
    return x * y

task1 = Task(name="task1", target=add, kwargs={"x": 1, "y": 5})
task2 = Task(name="task2", target=mul, args=(3, 4))

racer = Racer([task1, task2])
result = racer.run(1)
print(result)
```

Output:
```python
{0: {'task1': 6, 'task2': 12}}
```

### Running Parallel Task
To run a task in parallel, use the ParallelTask class. You can specify the number of workers (threads) to run the task concurrently.

```python
from racer import ParallelTask

def mul(x: int, y: int):
    return x * y

parallel_task = ParallelTask(name="task3", target=mul, num_workers=3, args=(5, 6))

racer = Racer([parallel_task])
result = racer.run(1)
print(result)
```

Output:
```python
{0: {'task3': [30, 30, 30]}}
```

### Passing the Previous Task’s Result to the Next Task
To pass the result of one task to the next, set the use_prev_result flag to True when defining the task. The framework will automatically pass the previous task’s result as an argument to the next task.

```python
def sub(x: int, y: int, prev_result=None):
    return prev_result - x - y

task1 = Task(name="task1", target=add, kwargs={"x": 1, "y": 5})
task2 = Task(name="task2", target=sub, args=(3, 4), use_prev_result=True)

racer = Racer([task1, task2])
result = racer.run(1)
print(result)
```
In this example, the result of task1 is passed as an additional argument to task2.

Output:
```python
{0: {'task1': 6, 'task2': -1}}
```

### Running with Clones
You can run the same set of tasks multiple times by passing the number of clones to the run method.

```python
racer = Racer([task1, task2])
result = racer.run(3)
print(result)
```

Output:
```python
{0: {'task1': 6, 'task2': -1}, 1: {'task1': 6, 'task2': -1}, 2: {'task1': 6, 'task2': -1}}
```

### More Examples
```python
from racer import ParallelTask, Racer, Task


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
        name="task3", target=mul, num_workers=3, args=(5, 6), use_prev_result=True
    )

    # tasks will be run sequentially
    racer = Racer([task1, task2, task3])
    results = racer.run(1)
    print(results)
```

Output:
```python
{0: {'task1': 6, 'task2': -7, 'task3': [30, 30, 30]}}
```
