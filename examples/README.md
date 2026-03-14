# Rinnsal Examples

This directory contains examples demonstrating various features of rinnsal.

## Examples

### 01_basic_tasks.py
Basic task and flow concepts: lazy evaluation, task chaining, and flow execution.

```bash
uv run python examples/01_basic_tasks.py
```

### 02_diamond_dependencies.py
Diamond-shaped DAG dependencies and automatic task deduplication.

```bash
uv run python examples/02_diamond_dependencies.py
```

### 03_retry_and_errors.py
Task retry on failure and error handling.

```bash
uv run python examples/03_retry_and_errors.py
```

### 04_flow_result_indexing.py
Rich FlowResult indexing: integer, string/regex, and callable-based filtering.

```bash
uv run python examples/04_flow_result_indexing.py
```

### 05_config_and_parameters.py
Using Config objects and flow parameters with defaults and overrides.

```bash
uv run python examples/05_config_and_parameters.py
```

### 06_caching_and_persistence.py
Result caching with FileDatabase and persistence across sessions.

```bash
uv run python examples/06_caching_and_persistence.py
```

### 07_subprocess_executor.py
Running tasks in separate processes for isolation and parallelism.

```bash
uv run python examples/07_subprocess_executor.py
```

### 08_progress_tracking.py
Progress bar and event-based progress callbacks.

```bash
uv run python examples/08_progress_tracking.py
```

### 09_ml_pipeline.py
A complete machine learning pipeline with hyperparameter search.

```bash
uv run python examples/09_ml_pipeline.py
```

### 10_cli_usage.py
CLI argument generation from flow signatures.

```bash
# Show help
uv run python examples/10_cli_usage.py --help

# Run with custom arguments
uv run python examples/10_cli_usage.py --learning-rate 0.1 --epochs 50
```

## Running All Examples

```bash
for f in examples/0*.py; do
    echo "=== Running $f ==="
    uv run python "$f"
    echo
done
```
