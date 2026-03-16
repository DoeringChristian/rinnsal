# Rinnsal

A declarative DAG execution framework for Python. Define tasks, compose them
into flows, and let rinnsal handle caching, deduplication, and execution.

## Installation

```bash
pip install rinnsal
```

Or with optional dependencies:

```bash
pip install rinnsal[ssh]  # SSH executor
pip install rinnsal[ray]  # Ray executor
pip install rinnsal[all]  # All extras
```

## Quick Start

```python
from rinnsal import task, flow

@task
def load_data():
    print("Loading data...")
    return [1, 2, 3, 4, 5]

@task
def process(data):
    print(f"Processing {len(data)} items...")
    return sum(data)

@task
def save(result):
    print(f"Saving result: {result}")
    return {"saved": result}

@flow
def my_pipeline():
    data = load_data()
    result = process(data)
    return save(result)

if __name__ == "__main__":
    result = my_pipeline()
    print(f"Final: {result[-1].result}")
```

Run with `-s` to see task output:

```bash
python my_script.py -s
```

## Features

### Tasks

Tasks are lazy - calling them returns a `TaskExpression`, not the result:

```python
@task
def compute(x):
    return x * 2

expr = compute(5)  # Returns TaskExpression, doesn't execute
result = expr.eval()  # Now it executes, returns 10
```

Tasks support retry on failure:

```python
@task(retry=3)
def flaky_operation():
    # Will retry up to 3 times on failure
    ...
```

### Flows

Flows compose tasks into a DAG:

```python
@flow
def pipeline():
    a = task_a()
    b = task_b(a)
    c = task_c(a)  # Diamond dependency - task_a runs once
    return task_d(b, c)
```

### FlowResult Indexing

Access tasks by position, name, or filter:

```python
result = my_flow()

# By position
first = result[0]
last = result[-1]

# By name (regex)
train_tasks = result["train_.*"]

# By argument value
fast_tasks = result[lambda lr: lr > 0.01]
```

### CLI Flags

Built-in flags work automatically:

```bash
python script.py -s              # Show task output (no capture)
python script.py --no-cache      # Disable caching
python script.py --executor subprocess  # Use subprocess executor
```

### Caching

Results are cached by content hash:

```python
from rinnsal.persistence.file_store import FileDatabase
from rinnsal.runtime.engine import ExecutionEngine, set_engine

db = FileDatabase(root=".rinnsal")
engine = ExecutionEngine(database=db, use_cache=True)
set_engine(engine)

# First run executes, second run loads from cache
result1 = my_flow()
result2 = my_flow()  # Instant - loaded from cache
```

### Executors

**InlineExecutor** (default): Runs in the same process.

**SubprocessExecutor**: Runs tasks in separate processes with code snapshots:

```python
from rinnsal.execution.subprocess import SubprocessExecutor
from rinnsal.runtime.engine import ExecutionEngine, set_engine

executor = SubprocessExecutor(max_workers=4)
set_engine(ExecutionEngine(executor=executor))
```

### Config Objects

Structured configuration with attribute access:

```python
from rinnsal import Config

config = Config(lr=0.01, epochs=100, batch_size=32)
print(config.lr)  # 0.01
print(config.to_dict())  # {"lr": 0.01, "epochs": 100, "batch_size": 32}
```

### Register/Build Pattern

Instantiate classes from config dicts:

```python
from rinnsal import register, build

@register
class MyModel:
    def __init__(self, hidden_size=256):
        self.hidden_size = hidden_size

config = {"type": "MyModel", "hidden_size": 512}
model = build(MyModel, config)
```

### Progress Bar

Flows show progress automatically:

```
████████████████░░░░░░░░░░░░░░░░░░░░░░░░ 2/5 2 passed [running: train]
```

Disable with:

```python
from rinnsal import set_progress
set_progress(False)
```

### Code Snapshots

Subprocess executors snapshot code by default, ensuring tasks use consistent
code even if files change during execution:

```python
executor = SubprocessExecutor(snapshot=True)  # Default
executor = SubprocessExecutor(snapshot=False)  # Disable
```

## Examples

See the `examples/` directory:

- `01_basic_tasks.py` - Tasks, flows, lazy evaluation
- `02_diamond_dependencies.py` - Deduplication
- `03_retry_and_errors.py` - Retry on failure
- `04_flow_result_indexing.py` - Rich indexing
- `05_config_and_parameters.py` - Config objects
- `06_caching_and_persistence.py` - FileDatabase
- `07_subprocess_executor.py` - Process isolation

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Run tests
pytest tests/

# Format code
black .

# Type check
mypy src/
```

## License

MIT
