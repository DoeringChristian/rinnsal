# Flow & Task System

A declarative DAG execution framework. You write plain Python functions,
decorate them with `@task`, wire them together in a `@flow`, and the system
handles execution order, caching, retries, CLI generation, and reproducibility.

## Tasks

A **task** is a lazy computation node. Decorating a function with `@task` makes
it return a `Task` object instead of executing immediately. Passing one task's
output as an argument to another creates a dependency edge — the system builds a
DAG automatically.

```python
@task
def source():
    return 10

@task
def double(x):
    return x * 2

double(source())  # No computation yet — just builds the graph
```

### Deduplication

Tasks are content-addressed. The same function called with the same arguments
always returns the same `Task` object. This means diamond dependencies are
naturally deduplicated — a shared dependency only runs once.

### Naming

Tasks can be given human-readable names for display and lookup:

```python
train("pretrain_data").name("pretrain")
train("finetune_data").name("finetune")
```

### Retry

Tasks support automatic retry on failure:

```python
@task(retry=3)
def flaky():
    ...
```

The task will be attempted up to 3 times before raising an error.

### Evaluation

Tasks can be evaluated standalone (outside a flow):

```python
result = pyexp.eval(double(source()))  # returns 20
```

`eval()` accepts one or more tasks. A single task returns its value; multiple
tasks return a tuple. Calling `str(task)` also triggers evaluation.

### Result History

Every successful task execution is persisted to a database. You can access the
full history:

```python
t = source()
t.eval()
t.runs       # Runs[Entry] — all previous executions
t.runs[-1]   # most recent Entry (has .result, .log, .metadata, .timestamp)
```

### Snapshots

When the executor is configured with `snapshot=True`, a hash of the source code
is captured at execution time. You can retrieve it later:

```python
t.snapshot        # Snapshot object, or None
t.snapshot.hash   # content hash string
t.snapshot.path   # Path to .snapshot/<hash>/ directory
```

## Flows

A **flow** wraps a function that builds a task DAG. The flow function doesn't
return tasks — it just calls them, and the system collects everything that was
registered.

```python
@flow
def my_flow(learning_rate=0.01, epochs=10):
    data = load_data()
    model = train(data, lr=learning_rate, epochs=epochs)
    evaluate(model)
```

### Running

```python
my_flow()                        # run with defaults
my_flow(learning_rate=0.001)     # override via Python
```

Or from the CLI:

```bash
python my_script.py --learning-rate 0.001 --epochs 20
```

The flow auto-generates CLI flags from the function signature with type coercion
(int, float, bool, str). CLI flags take priority over Python-level overrides,
which take priority over defaults.

Built-in CLI flags:

- `--executor NAME` — select an executor by name
- `--spin TASK_NAME` — re-run only one task (see Spin Mode)
- `-s` / `--no-capture` — disable stdout/stderr capture

### FlowResult

Running a flow returns a `FlowResult` — an indexable collection of all evaluated
tasks.

```python
result = my_flow()
```

**Integer indexing** — positional access:

```python
result[0]    # first task
result[-1]   # last task
```

**String indexing** — regex match on task name or function name:

```python
result["train"]         # exact or partial match
result["train_.*"]      # regex pattern
result["pretrain"]      # single match returns Task directly
```

If a string matches multiple tasks, a new `FlowResult` is returned containing
just those tasks.

**Callable indexing** — filter by task input arguments:

```python
result[lambda lr: lr == 0.001]
result[lambda dataset, mode: dataset == "imagenet" and mode == "train"]
```

The lambda's parameter names are matched against each task's resolved input
arguments. Tasks whose signatures don't contain those parameter names are
silently skipped — so mixed-signature flows work naturally. A single match
returns the Task; multiple matches return a new `FlowResult`.

### Historical Results

Flow results are persisted. You can load any previous run without re-executing:

```python
my_flow[-1]   # most recent run
my_flow[0]    # first run
my_flow[-1]["pretrain"].result  # access a specific task's result from history
```

All indexing modes (int, str, callable) work on historical results too.

### Re-collecting Results

`flow.results()` re-runs the flow function to rebuild the DAG (without
executing), then loads cached results for each task from the database:

```python
result = my_flow.results()
result = my_flow.results(learning_rate=0.001)  # rebuild with specific kwargs
```

This is useful when you want live `Task` objects (with access to `.runs`,
`.snapshot`, etc.) without re-executing.

### Spin Mode

Spin re-runs a single named task while loading all others from cache. Useful for
iterating on one step of a pipeline:

```bash
python my_script.py --spin evaluate
```

The targeted task is re-executed (with retry support); all other tasks load
their most recent cached result. Requires a previous full run.

### Snapshots

Flow results also carry snapshot information when the executor provides it:

```python
result = my_flow()
result.snapshot         # Snapshot from the flow run
my_flow[-1].snapshot    # snapshot from a historical run
```

### Self-Containment

Flows are self-contained. Tasks created inside a flow are cleaned up from the
global registry after the flow completes. Running a flow, then calling
`flow.results()`, or running it again, will never conflict.

## Persistence

### Database

Results are stored via a `Database` protocol with two operations: Figure out a
appropriate interface, that also allows for remote synchronization.

### Runs Collection

`task.runs` returns a `Runs[Entry]` collection that supports the same rich
indexing as `FlowResult` — integer, regex string, and dict-based filtering.

## Progress

During flow execution, an ANSI progress bar renders to stderr showing:

- A visual bar with completion percentage
- Counts of passed/failed/cached tasks
- The name of the currently running task

## Executors

Executors are responsible for running task functions and returning their
results. Each executor manages a **pool** of workers — threads, processes, or
remote connections — and tasks are submitted to this pool for execution.

All executors support:

- **Output capture** — optionally intercept stdout/stderr during task execution
  so that logs are stored alongside results
- **Code snapshots** — optionally hash and package the current source tree
  before execution, enabling exact reproducibility

### Execution Environments

**Inline** — runs tasks in the calling process. Simple, no serialization
overhead. Useful for debugging and lightweight pipelines.

**Subprocess / Fork** — runs tasks in separate OS processes on the same machine.
Provides isolation (crashes don't take down the orchestrator) and true
parallelism. The subprocess variant spins up fresh Python interpreters; fork
shares memory at the point of fork.

**SSH** — runs tasks on remote machines over SSH. The executor manages a pool of
SSH connections to one or more hosts. Code snapshots are transferred to the
remote before execution, and results are transferred back. This requires that
inputs, outputs, and the function itself are all serializable across the wire.

**Ray** — distributes tasks across a Ray cluster. Leverages Ray's own
scheduling, object store, and fault tolerance. Suitable for large-scale
distributed execution.

### Executor–Database Interaction

For remote executors (SSH, Ray, etc.), task inputs and outputs must cross
machine boundaries. This has implications for the database:

- **Result transfer** — when a task runs remotely, its result must be sent back
  to the orchestrator (or to a shared store) so that downstream tasks and the
  database can access it. The database interface should accommodate both local
  writes and results arriving from remote workers.
- **Input transfer** — when a task depends on the output of another task that
  ran on a different machine, that result must be available where the new task
  executes. This can happen via the orchestrator forwarding the data, a shared
  filesystem, or a remote-aware database that workers can read from directly.
- **Synchronization** — multiple workers may write results concurrently. The
  database must handle concurrent writes safely, whether through file locking,
  atomic operations, or a centralized service.

The database protocol should be designed so that local-only execution
(file-based storage) and remote execution (shared/synced storage) both work
without changing task code.

## Schedulers

The scheduler decides **which executor** runs **which task** and **when**. It
sits between the DAG engine and the executor pool, making placement and ordering
decisions.

### Responsibilities

- **Task placement** — assign each ready task to a specific executor or worker
  from the available pool. A simple scheduler round-robins across workers; a
  smarter one considers constraints.
- **Ordering** — beyond topological order, the scheduler can reorder independent
  tasks to optimize resource usage (e.g., start GPU-heavy tasks first, or batch
  small tasks together).
- **Concurrency control** — decide how many tasks run in parallel, respecting
  executor pool sizes and resource limits.

### Optimization Objectives

Different scheduling strategies optimize for different things:

- **Minimize data transfer** — when tasks run on different machines, upstream
  results must be shipped to where the downstream task runs. A
  data-locality-aware scheduler places dependent tasks on the same worker when
  possible, reducing network traffic. This matters most for large intermediate
  results (datasets, model weights).
- **Maximize parallelism** — schedule independent branches of the DAG
  concurrently across all available workers, even if it means some data
  transfer. Prioritizes wall-clock time over network efficiency.
- **Resource matching** — assign tasks to workers that have the right resources
  (GPU, high memory, specific hardware). Tasks can declare resource
  requirements, and the scheduler matches them to capable workers.
- **Load balancing** — distribute work evenly across workers, avoiding
  situations where one machine is saturated while others are idle. Takes into
  account estimated task duration and current worker load.
- **Cost minimization** — for cloud or pay-per-use resources, prefer cheaper
  workers when possible, scale down idle workers, and batch tasks to reduce
  instance-hours.

A scheduler can combine these objectives with different weights depending on the
workload. For example, a training pipeline with large checkpoints would favor
data locality, while a hyperparameter sweep with many small independent tasks
would favor maximum parallelism.

### Interaction with Executors

The scheduler does not execute tasks itself — it produces a plan (task-to-worker
assignments) that the DAG engine carries out via the executors. The scheduler
needs visibility into:

- The full DAG structure (what depends on what)
- Estimated or known result sizes (for data transfer cost)
- Worker capabilities and current load (for placement decisions)
- Task resource requirements (if declared)

## Example

```python
import pyexp
from pyexp import Config

@pyexp.task
def exp(config: Config, pretrain=None):
    lr = config.learning_rate
    epochs = config.epochs
    return {"accuracy": 0.9 + lr * epochs / 100}

@pyexp.flow
def pipeline():
    datasets = ["imagenet"]

    for dataset in datasets:
        pretrain = exp(
            Config({"learning_rate": 0.01, "epochs": 10, "dataset": dataset}),
        ).name(f"{dataset}_pretrain")

        exp(
            Config({"learning_rate": 0.01, "epochs": 10, "dataset": dataset, "finetune": True}),
            pretrain=pretrain,
        ).name(f"{dataset}_finetune")

if __name__ == "__main__":
    pipeline()

    # Load historical results
    last = pipeline[-1]
    print(last["pretrain"].result)

    # Filter by input arguments
    results = pipeline.results()
    print(results[lambda config: config.learning_rate == 0.01])
```
