# Runtime Management

A `noleme-flow` DAG is not just a static structure; it interacts with its environment at runtime through input and output management. This chapter explains how to provide data to your flows and how to retrieve results.

## Runtime Input

You can provide inputs to a flow at the moment of execution. This is particularly useful for parameterizing your DAGs.

### The `Input` Component

The `Input` component acts as a container for values that can be accessed by the flow's nodes. You can create an `Input` and fill it with values associated with keys or string identifiers.

```java
// Using keys
var myKey = Input.key(String.class);
var input = Input.of(myKey, "some value");

// Using identifiers
var input2 = Input.of("my_id", 42)
    .and("another_id", "hello");
```

### Dynamic `from`

You can start a flow branch that expects an input from the `Input` component using `Flow.from()`.

```java
var flow = Flow.from("input_identifier")
    .pipe(String::toUpperCase);

// At runtime:
var input = Input.of("input_identifier", "hello");
var output = Flow.runAsPipeline(input, flow);
```

## Runtime Output

Retrieving data from a running flow is done through the `Output` component and the concept of `Recipient`.

### Collecting Results

To mark a node's output for collection, use the `collect()` method. It returns a `Recipient` which can be used to retrieve the value from the `Output` object after the flow has run.

```java
var flow = Flow.from(() -> 42);
var recipient = flow.collect();

var output = Flow.runAsPipeline(flow);
Integer value = output.get(recipient); // value is 42
```

You can also name your collections for easier retrieval:

```java
flow.collect("my_result");

var output = Flow.runAsPipeline(flow);
Integer value = (Integer) output.get("my_result");
```

### Sampling (Experimental)

Sampling allows you to inspect the data flowing through a node without necessarily making it a final output of the flow. This is often used for debugging or monitoring.

```java
// Note: sampling features are often implementation-specific or experimental.
// Check the specific FlowRuntime for sampling capabilities.
```

### The `Output` Object

The `Output` object returned by `Flow.runAs...` contains:
* The collected values (accessible via `Recipient` or identifier).
* Execution metadata, such as `startTime()`, `endTime()`, and `elapsedTime()`.

```java
var output = Flow.runAsPipeline(flow);
System.out.println("Execution took: " + output.elapsedTimeString());
```
