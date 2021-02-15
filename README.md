# Lumio Flow

[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/lumio-medical/lumio-flow/Java%20CI%20with%20Maven)](https://github.com/lumio-medical/lumio-flow/actions?query=workflow%3A%22Java+CI+with+Maven%22)
[![Maven Central Repository](https://maven-badges.herokuapp.com/maven-central/com.lumiomedical/lumio-flow/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.lumiomedical/lumio-flow)
[![javadoc](https://javadoc.io/badge2/com.lumiomedical/lumio-flow/javadoc.svg)](https://javadoc.io/doc/com.lumiomedical/lumio-flow)
![GitHub](https://img.shields.io/github/license/lumio-medical/lumio-flow)

This library provides an opinionated way of structuring data processing programs such as ETLs.

Implementations found in this package will shouldn't be tied to any specific Lumio project.

_Note: This library is considered as "in beta" and as such significant API changes may occur without prior warning._

## I. Installation

Add the following in your `pom.xml`:

```xml
<dependency>
    <groupId>com.lumiomedical</groupId>
    <artifactId>lumio-flow</artifactId>
    <version>0.12</version>
</dependency>
```

## II. Notes on Structure and Design

The core idea behind this library is to structure the program as a DAG of "actions" to perform, then compile that graph into concrete runnable instance which properties will depend on the implementation.
 
These actions can be of three different types, mirroring an ETL process:

* `Extractor` for introducing data into the flow, they would typically be connectors for a variety of data sources 
* `Transformer` for manipulating data and returning an altered version of it, or new data inferred from the input
* `Loader` for dumping data out of the flow

Additionally, there are two actions related to stream flows:

* `Generator` for gradually introducing data into the flow, they can be iterating through a `Collection`, reading off an `InputStream` or generating data on-the-fly
* `Accumulator` for doing the reverse operation, they accumulate all outputs from a stream flow and continue with a standard (ie. non-stream) flow

Once a DAG has been defined, a `FlowCompiler` will be responsible for transforming the DAG representation into a runnable instance, a `FlowRuntime`. 

At the time of this writing, there are two available implementations:

* the serial `PipelineRuntime` which will run one node after another, making sure each one can satisfy its input
* the parallel `ParallelRuntime` which will attempt to run any node that can satisfy its input in a parallel fashion

Once a `FlowRuntime` has been produced, we can simply `run` it.

_TODO_

## III. Usage

Here is a very basic example of pipeline we could create:

```java
/* We initialize a flow */
var flow = Flow
    .from(() -> 1)
    .pipe(i -> i + 1)
    .pipe(i -> i * 2)
    .sink(System.out::println)
;

/* We run it as a Pipeline */
Flow.runAsPipeline(flow);
```

Which, upon running should print `4`.

Another example:

```java
/* We initialize a flow */
var flow = Flow
    .from(() -> 2)
    .pipe(i -> i * 2)
;

/* We branch the flow in two branchs */
var branchA = flow.pipe(i -> i * i);
var branchB = flow.pipe(i -> i * 5);

/* We join the two branchs and collect the end result */
var recipient = branchA
    .join(branchB, Integer::sum)
    .pipe(i -> i * 2)
    .collect()
;

var output = Flow.runAsPipeline(flow);

System.out.println(output.get(recipient));
```

Upon running this should print `72` (`2*((2*2)^2)+((2*2)*5)`).

Now a final example with a stream flow going on:

```java
/* Let's have a "standard" flow doing its thing */
var branch = Flow
    .from(() -> 2)
    .pipe(i -> i + 1)
;

/* Create a "stream" flow from a list of integers  */
var flow = Flow
    .from(() -> List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    .stream(IterableGenerator::new)
    .pipe(i -> i * i)
    .join(branch, (f, b) -> f * b) /* All values in the main flow will be multiplied by the output from the branch flow */
    .accumulate(values -> values.stream() /* Once the generator is exhausted and all stream nodes have run, we gather the output integers and sum them ; note that accumulation is optional (you could also end the stream with a sink) */
        .reduce(Integer::sum)
        .orElseThrow(() -> new AccumulationException("Could not sum data."))
    )
    .pipe(i -> i + 1) /* After the accumulation step, the flow is back to being a "standard" flow so we can queue further transformations */
    .sink(System.out::println)
;

Flow.runAsPipeline(flow);
```

Upon running this should print `856`.

Note that `lumio-flow` doesn't provide any `Generator` implementation, but the IterableGenerator class mentioned above could be implemented the following way:

```java
public class IterableGenerator<T> implements Generator<T>
{
    private final Iterator<T> iterator;

    public IterableGenerator(Iterable<T> iterable)
    {
        this.iterator = iterable.iterator();
    }

    @Override
    public boolean hasNext()
    {
        return this.iterator.hasNext();
    }

    @Override
    public T generate()
    {
        return this.iterator.hasNext() ? this.iterator.next() : null;
    }
}
``` 

Other features that will need to be documented include:

* the complete set of DAG building methods (including alternate flavours of `from`, `stream`, as well as `driftSink`, `after` and the generic `into`)
* control-flow with partial DAG interruption (`interrupt` and `interruptIf`)
* runtime input management (dynamic `from` and the `Input` component) 
* runtime output management, sampling/collection features (`collect`, `sample` and the `Output` component)
* stream flows and parallelization (`setMaxParallelism` and implementation-specific considerations)
* `ParallelRuntime` service executor lifecycle and other considerations

_TODO_

## IV. Dev Installation

This project will require you to have the following:

* Java 11+
* Git (versioning)
* Maven (dependency resolving, publishing and packaging) 
