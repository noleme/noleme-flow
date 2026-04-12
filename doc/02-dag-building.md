# Building your Flow DAG

The `Flow` class serves as the entry point for building your data processing graph. This chapter covers the fundamental methods used to structure your DAG.

## 1. Introducing Data

You can introduce data into your flow using the `from()` and `stream()` methods.

### `from`

`from()` is the standard way to start a flow from an `Extractor` or a fixed input.

```java
// From a simple Extractor
var flow = Flow.from(() -> "Start data");

// From a Transformer that takes a fixed input
var flow2 = Flow.from(String::toUpperCase, "hello");
```

### `stream`

`stream()` starts a stream flow from a `Generator`. This is useful for processing data that can be generated or iterated over.

```java
// From a simple list of integers
var flow = Flow.stream(() -> new IterableGenerator<>(List.of(1, 2, 3)));
```

## 2. Transforming and Pushing Data

Once data is in the flow, you can process it using `pipe()` and `sink()`.

### `pipe`

`pipe()` is used to apply a `Transformer` to the data coming from an upstream node.

```java
var flow = Flow.from(() -> 1)
    .pipe(i -> i + 1);
```

### `sink`

`sink()` is used to end a branch of the flow by applying a `Loader`. It doesn't return a value that can be further processed.

```java
var flow = Flow.from(() -> 1)
    .sink(System.out::println);
```

## 3. Advanced Node Operations

### `join`

`join()` allows you to merge two different branches of your DAG by applying a `BiTransformer`.

```java
var branchA = Flow.from(() -> 10);
var branchB = Flow.from(() -> 20);

var flow = branchA.join(branchB, (a, b) -> a + b);
var result = flow.collect();

var output = Flow.runAsPipeline(branchA, branchB);
Integer value = output.get(result);
```

### `into`

`into()` is a more generic alternative to `pipe()` and `sink()`. It allows you to specify a node's connection explicitly.

```java
var source = Flow.from(() -> "Hello");
var pipe = Flow.into(source, String::toUpperCase);
```

### `driftSink`

`driftSink()` is a special method that allows you to attach a `Loader` to a node without ending its branch. The data continues to flow to subsequent nodes as if the `driftSink` weren't there. This is useful for logging or side-effects.

```java
var flow = Flow.from(() -> "data")
    .driftSink(System.out::println) // Data still flows to the next pipe
    .pipe(String::toUpperCase);

var result = flow.collect();
var output = Flow.runAsPipeline(flow);
```

### `after`

The `after()` method allows you to specify execution order between nodes that do not have a direct data dependency. This is crucial for ensuring that certain side-effects or initialization steps happen before a node runs.

```java
var nodeA = Flow.from(() -> { /* perform initialization */ return null; });
var nodeB = Flow.from(() -> "Main data");

nodeB.after(nodeA); // nodeB will only run after nodeA has finished
```
