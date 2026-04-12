# Streams & Parallelism

`noleme-flow` provides specialized support for processing streams of data and for executing DAG nodes in parallel.

## Stream Flows

A stream flow is initiated using a `Generator` via the `Flow.stream()` or `FlowOut.stream()` methods. Unlike a standard flow that processes a single data item per node, a stream flow processes multiple items sequentially or in parallel.

### Generators and Accumulators

* **`Generator`**: Produces a sequence of items to be processed by the flow.
* **`Accumulator`**: Collects the processed items from the stream and transitions back to a standard flow.

```java
var flow = Flow.stream(() -> new IterableGenerator<>(List.of(1, 2, 3)))
    .pipe(i -> i * 2)
    .accumulate(input -> input); // Transitions back to a standard flow containing a Collection<Integer>
```

## Parallel Execution

Parallelization can be achieved in two ways: by using the `ParallelRuntime` and by configuring parallelism on stream generators.

### `ParallelRuntime`

The `ParallelRuntime` attempts to execute any node whose dependencies are met in parallel. This is the simplest way to gain performance for independent branches of your DAG.

```java
Flow.runAsParallel(4, flow); // Run with a thread pool of 4 threads
```

### `setMaxParallelism`

For stream flows, you can control the level of parallelism for the stream processing using `setMaxParallelism(int factor)`. This determines how many items from the stream can be processed concurrently.

```java
var flow = Flow.stream(() -> new MyLargeGenerator())
    .setMaxParallelism(4) // Process up to 4 items in parallel
    .pipe(someExpensiveTransformation);
```

### Implementation Considerations

* **Thread Safety**: When running in parallel, ensure that your `Transformer` and `Loader` implementations are thread-safe, especially if they share state.
* **`ParallelRuntime` Lifecycle**: The `ParallelRuntime` manages a thread pool. By default, it uses a fixed thread pool. You can provide your own `ExecutorServiceProvider` if you need more control over the executor's lifecycle or configuration.
* **Blocking Operations**: Be cautious with blocking operations in a parallel flow, as they can quickly exhaust the thread pool.
