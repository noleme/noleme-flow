# Lumio Flow

[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/lumio-medical/lumio-flow/Java%20CI%20with%20Maven)](https://github.com/lumio-medical/lumio-flow/actions?query=workflow%3A%22Java+CI+with+Maven%22)
[![Maven Central Repository](https://maven-badges.herokuapp.com/maven-central/com.lumiomedical/lumio-flow/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.lumiomedical/lumio-flow)
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
    <version>0.7.1</version>
</dependency>
```

## II. Notes on Structure and Design

The core idea behind this library is to structure the program as a DAG of "actions" to perform, then compile that graph into concrete runnable instance which properties will depend on the implementation.
 
These actions can be of three different types, mirroring an ETL process:

* `Extractor` for introducing data into the flow, they would typically be connectors for a variety of data sources 
* `Transformer` for manipulating data and returning an altered version of it, or new data inferred from the input
* `Loader` for dumping data out of the flow

Once a DAG has been defined, a `FlowCompiler` will be responsible for transforming the DAG representation into a runnable instance, a `FlowRuntime`. 

At the time of this writing, there are two available implementations:

* the serial `PipelineRuntime` which will run one node after another, making sure each one can satisfy its input
* the parallel `ParallelPipelineRuntime` which will attempt to run any node that can satisfy its input in a parallel fashion

Once a `FlowRuntime` has been produced, we can simply `run` it.

_TODO_

## III. Usage

Here is a very basic example of pipeline we could create:

```java
/* We initialize a flow */
var flow = Flow
    .from(() -> 1)
    .into(i -> i + 1)
    .into(i -> i * 2)
    .into((Loader<Integer>) System.out::println)
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
    .into(i -> i * 2)
;

/* We branch the flow in two branchs */
var branchA = flow.into(i -> i * i);
var branchB = flow.into(i -> i * 5);

/* We join the two branchs and collect the end result */
var recipient = branchA
    .join(branchB, Integer::sum)
    .into(i -> i * 2)
    .collect() /* The collect operation is implemented as a stateful Loader */
;

Flow.runAsPipeline(flow);

System.out.println(recipient.getContent());
```

Upon running this should print `72` (`2*((2*2)^2)+((2*2)*5)`).

_TODO_

## IV. Dev Installation

This project will require you to have the following:

* Java 13+
* Git (versioning)
* Maven (dependency resolving, publishing and packaging) 
