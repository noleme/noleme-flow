# Flow Slices

One of the key features of `noleme-flow`'s composition model is the ability to bundle parts of a DAG into reusable fragments called "Slices". This allows for modularity and high levels of code reuse.

## What is a Slice?

A Slice is essentially a factory for a sub-DAG. It encapsulates a sequence of nodes and their configuration, exposing only the entry and exit points.

There are three main types of slices:

* **`SourceSlice<O>`**: Represents a fragment that starts a flow branch.
* **`PipeSlice<I, O>`**: Represents a fragment that transforms data (has an input and an output).
* **`SinkSlice<I>`**: Represents a fragment that ends a flow branch.

## Creating a Slice

To create a slice, you extend one of the abstract slice classes and implement its `out()` method.

```java
public class MyTransformationSlice extends PipeSlice<String, Integer> {
    @Override
    public CurrentOut<Integer> out(CurrentOut<String> upstream) {
        return upstream
            .pipe(String::trim)
            .pipe(String::length);
    }
}
```

## Using Slices

Slices are integrated into a flow using the `from()`, `pipe()`, or `sink()` methods.

```java
var mySlice = new MyTransformationSlice();

var flow = Flow.from(() -> "  hello  ")
    .pipe(mySlice) // The logic inside MyTransformationSlice is injected here
    .sink(System.out::println);
```

### Why use Slices?

* **Modularity**: Encapsulate complex logic into a single, manageable component.
* **Reusability**: Use the same transformation or source across different DAGs.
* **Testing**: Test your slices in isolation before integrating them into a larger flow.
* **Readability**: Keep your main flow definition clean and high-level by offloading details to slices.

Slices are a powerful tool for building complex, maintainable data pipelines by composing smaller, well-defined building blocks.
