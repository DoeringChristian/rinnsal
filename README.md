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

Tasks can have a timeout to prevent hanging:

```python
@task(timeout=60)
def gpu_train(data):
    ...  # Killed after 60s if stuck
```

Tasks can catch failures and continue with a default value:

```python
@task(catch=True)
def risky(x):
    ...  # Returns None on failure, downstream continues

@task(catch="fallback")
def maybe(x):
    ...  # Returns "fallback" on failure
```

`catch` applies after retries are exhausted: `@task(retry=3, catch=True)` retries 3 times,
then returns `None` if all attempts fail.

### Fan-out with `.map()`

Apply a task to each element of an iterable:

```python
@task
def process(item):
    return item * 2

results = process.map([1, 2, 3])        # 3 TaskExpressions
pairs = add.map([1, 2], [10, 20])       # Multi-arg: zip and apply
```

Each mapped task is auto-named `process[0]`, `process[1]`, etc.

### Resource Declarations

Declare compute resources a task needs:

```python
from rinnsal import Resources

@task(resources=Resources(gpu=1, gpu_memory=16000))
def train(data): ...

@task(resources={"gpu_memory": 8000})  # Dict shorthand
def evaluate(model): ...
```

Resources are stored in execution metadata and used by the scheduler
to match tasks to workers.

### Cards

Attach rich content to task results for inspection:

```python
from rinnsal import current

@task
def train(data):
    model = fit(data)
    current.card.text(f"Accuracy: {model.accuracy}")
    current.card.image(model.loss_plot(), title="Loss Curve")
    current.card.table(metrics, headers=["Epoch", "Loss", "Acc"])
    return model
```

Card data is stored in the Entry metadata and viewable in the web UI.

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

### Task Capture

By default, flows capture **all** tasks created inside the flow body and evaluate
them on `.run()`, even if they aren't part of the return value. This is useful for
side-effect tasks like logging, checkpointing, or metrics:

```python
@flow
def pipeline():
    data = load_data()
    model = train(data)

    # These run automatically — no need to return them
    log_metrics(model)
    save_checkpoint(model)

    return model
```

To opt out and only evaluate returned tasks:

```python
@flow(capture_tasks=False)
def pipeline():
    data = load_data()
    model = train(data)
    log_metrics(model)       # will NOT run
    save_checkpoint(model)   # will NOT run
    return model
```

### Task History

Access previous execution results for any task expression via `.runs`:

```python
expr = train(data, lr=0.01)

runs = expr.runs        # Runs collection, chronological order
runs[-1].result         # most recent result
runs[0].result          # oldest result
len(runs)               # number of historical runs
```

Results are persisted to disk automatically and available across sessions.

### CLI Flags

Built-in flags work automatically:

```bash
python script.py -s                          # Show task output (no capture)
python script.py --executor subprocess       # Select executor
python script.py --filter "train.*"          # Only run matching tasks
python script.py --resume                    # Re-run only failed tasks
python script.py --dry-run                   # Print DAG without executing
python script.py --tag experiment-v2         # Tag the run (repeatable)
python script.py --snapshot abc123           # Run using a previous snapshot
python script.py --snapshot-from my_flow     # Use snapshot from latest run
python script.py --db-path .rinnsal          # Database directory
```

### Task Filtering

Re-run specific tasks without re-executing the entire flow:

```bash
# Only execute tasks matching the pattern
python script.py --filter "train.*"
```

When using `--filter`:
- Tasks matching the regex pattern are executed
- Dependencies of matched tasks are loaded from cache
- Non-matching tasks that aren't dependencies are skipped

This requires a previous full run to populate the cache for dependencies.

### Resuming Failed Runs

When a flow partially fails, re-run only the failures:

```bash
python script.py            # First run: some tasks fail
python script.py --resume   # Re-run only failed tasks, load successes from cache
```

Combine with `--filter` to narrow further:

```bash
python script.py --resume --filter "train.*"  # Only retry failed training tasks
```

### Dry Run

Print the task DAG without executing:

```bash
python script.py --dry-run
# Flow: my_pipeline
# Tasks (3):
#   load_data
#   process <- load_data
#   save <- process
```

### Run Tagging

Tag flow runs for organization and later filtering:

```bash
python script.py --tag experiment-v2 --tag gpu-a100
```

Tags are stored in flow run metadata and can be used to filter past runs.

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

**SSHExecutor**: Runs tasks on remote machines over SSH. The remote host
needs Python 3 and `cloudpickle` installed:

```python
from rinnsal.execution.ssh import SSHExecutor, SSHHost
from rinnsal.runtime.engine import ExecutionEngine, set_engine

hosts = [
    SSHHost("gpu-server", username="alice"),
    SSHHost("gpu-server-2", username="alice", key_path="~/.ssh/id_ed25519"),
]
executor = SSHExecutor(hosts=hosts)
set_engine(ExecutionEngine(executor=executor))
```

Tasks are round-robined across hosts. Use multiple hosts to distribute work.

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

### Snapshot Replay

Run code against a previous snapshot. Useful when you've modified code
but want to inspect results from an earlier run using the original modules:

```python
from rinnsal import use_snapshot

# Use the snapshot from the latest run of a flow
with use_snapshot(flow="my_training_flow"):
    from my_module import viewer
    viewer.show(result)

# Or by snapshot hash directly
with use_snapshot(hash="abc123def456"):
    import my_module
    my_module.inspect(data)
```

Also available as CLI flags or flow parameters:

```bash
python view.py --snapshot-from my_training_flow
python view.py --snapshot abc123
```

```python
view_flow().run(snapshot_from="my_training_flow")
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
- `08_logger.py` - Logging scalars, text, and figures
- `09_independent_task.py` - Tasks outside flows, `.runs` history
- `10_capture_tasks.py` - Automatic task capture in flows
- `11_ssh_executor.py` - Remote execution over SSH
- `12_filtering.py` - Task filtering with `--filter`
- `13_slurm_executor.py` - Slurm cluster submission
- `14_slurm_sweep.py` - Hyperparameter sweep on Slurm with `.map()`
- `15_checkpointing.py` - Resumable tasks with `current.checkpoint`
- `16_cards.py` - Rich task output with `current.card`

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
