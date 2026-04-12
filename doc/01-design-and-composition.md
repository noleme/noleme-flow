# Design & Composition

`noleme-flow` is designed around the idea that data processing logic should be expressed as a Directed Acyclic Graph (DAG) of discrete "actions". This approach allows for a clean separation between the definition of the flow and its execution, while enabling high levels of code reuse through a powerful composition model.

## The Core Concept: Actions as Nodes

At its heart, a flow is a collection of nodes, each representing a specific action in your data pipeline. These actions mirror the standard ETL (Extract, Transform, Load) process:

* **`Extractor`**: Responsible for introducing data into the flow. It typically acts as a connector to external data sources (databases, files, APIs).
* **`Transformer`**: Manipulates data received from upstream nodes. It can alter the data, filter it, or produce entirely new data based on its input.
* **`Loader`**: Handles the output of the flow, dumping data into a destination (e.g., writing to a file, updating a database).

Beyond these core types, `noleme-flow` introduces specialized actions for handling streams:

* **`Generator`**: Gradually introduces data into the flow (e.g., iterating through a collection or reading an `InputStream`).
* **`Accumulator`**: Collects all outputs from a stream flow and transitions back to a standard flow once the stream is exhausted.

## The Composition Model: Pipelines as Building Blocks

The true power of `noleme-flow` lies in its **composition model**. A pipeline is not just a sequence of steps; it is a node itself that can be reused and integrated into larger, more complex graphs.

### Nodes and Pipelines

Every step you define in a flow results in a node. By chaining these nodes using methods like `.pipe()`, `.join()`, or `.into()`, you are building a graph.

```java
var flow = Flow
    .from(() -> "hello")
    .pipe(String::toUpperCase)
    .sink(System.out::println);
```

In this example, each call adds a node to the graph. But what if you have a complex sequence of transformations that you want to use in multiple places?

### Reusability through Slices

`noleme-flow` provides "Slices" (`SourceSlice`, `PipeSlice`, `SinkSlice`) which allow you to bundle fragments of a DAG. These slices can then be injected into other flows, making your data processing logic highly modular and reusable.

Think of it as building small, specialized pipelines that can be snapped together to form a larger system. This encourages:

* **DRY (Don't Repeat Yourself)**: Define common transformation logic once and reuse it across different flows.
* **Testability**: Small, focused slices are easier to test in isolation.
* **Readability**: High-level flows become easier to understand when they are composed of well-named, logical components.

## Compilation and Runtime

Once you have defined your DAG, it needs to be "compiled" into a runnable instance. This is handled by a `FlowCompiler`.

The compiler analyzes the graph and produces a `FlowRuntime`. Depending on the compiler used, the runtime can exhibit different execution properties:

* **`PipelineRuntime`**: Executes nodes serially, ensuring each node's input is satisfied before it runs.
* **`ParallelRuntime`**: Attempts to run nodes in parallel whenever their dependencies are met, leveraging multi-core architectures for better performance.

This separation of definition and execution means you can define your logic once and decide at runtime whether it should run serially or in parallel, simply by choosing a different compiler.

To see how these concepts are applied in the real world with concrete connectors, check the [Practical Usage & Connectors](08-practical-usage-and-connectors.md) chapter.
