# Advanced Features

This chapter covers additional features of `noleme-flow` that are useful for debugging, monitoring, and fine-grained control of your DAG.

## Node Naming

By default, nodes in a flow are assigned a unique identifier (UID). However, you can also give them human-readable names. This is highly recommended for complex DAGs, as these names will appear in logs, traces, and debugging tools, making it much easier to identify which part of the flow is executing.

```java
var flow = Flow.from(() -> "data")
    .name("Source: Raw Data")
    .pipe(String::toUpperCase)
    .name("Transform: To Upper");
```

## Sampling

Sampling is a way to "peek" at the data passing through a node without affecting the flow's execution. It's essentially a shortcut for `collect()` that doesn't return a `Recipient` but instead allows you to retrieve the value later using its name from the `Output` object.

```java
var flow = Flow.from(() -> "secret data")
    .sample("raw_data_peek") // Same as collect("raw_data_peek") but returning the node for chaining
    .pipe(data -> "transformed " + data);
```

## Implementation-Specific Considerations

The behavior of certain features may depend on the `FlowRuntime` being used.

### `ParallelRuntime` Service Executor

The `ParallelRuntime` uses an `ExecutorService` to manage its thread pool. You can customize this by providing your own `ExecutorServiceProvider`. This allows you to:
* Use a different type of thread pool (e.g., cached thread pool, scheduled thread pool).
* Monitor the thread pool's state.
* Control the thread pool's lifecycle (e.g., sharing it across multiple runtimes).

```java
// Example of providing a custom executor service provider
ExecutorServiceProvider provider = () -> Executors.newFixedThreadPool(10);
var output = Flow.runAsParallel(provider, input, flow);
```

Remember to properly shut down your executor services if you are managing them yourself!
