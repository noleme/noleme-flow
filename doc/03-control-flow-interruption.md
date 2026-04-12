# Control-flow & Interruption

`noleme-flow` provides mechanisms to manage the execution flow of your DAG, including partial interruption and error handling.

## Interruption

You can interrupt the execution of a flow branch based on certain conditions without stopping the entire flow.

### `interrupt` and `interruptIf`

These methods allow you to stop the processing of subsequent nodes in the current branch.

* `interrupt()`: Unconditionally interrupts the branch.
* `interruptIf(Predicate<O> predicate)`: Interrupts the branch if the provided predicate evaluates to `true`.

```java
var flow = Flow.from(() -> 10)
    .interruptIf(i -> i > 5)
    .pipe(i -> i * 2); // This will not run because i is 10
```

When an interruption occurs, any node that depends on the interrupted node will not be executed. However, other independent branches of the DAG will continue to run as normal.

## Error Handling

By default, an exception thrown during node execution will cause the flow to fail. `noleme-flow` provides helpers to manage exceptions more gracefully.

### `nonFatal`

The `nonFatal` helper allows you to wrap an action so that any exceptions it throws are caught and transformed into an interruption. This ensures that a failure in one node doesn't necessarily halt the entire pipeline.

```java
var flow = Flow.from(Flow.nonFatal(() -> {
    throw new RuntimeException("Something went wrong");
}))
.pipe(i -> i + 1); // This will not run because of the interruption
```

You can also provide a custom exception handler:

```java
var flow = Flow.from(Flow.nonFatal(
    () -> { throw new RuntimeException("Error"); },
    e -> System.err.println("Caught error: " + e.getMessage())
));
```
