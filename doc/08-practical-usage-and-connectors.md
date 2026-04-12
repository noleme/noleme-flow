# Practical Usage & Connectors

While `noleme-flow` provides the core DAG and runtime engine, its true power is realized when combined with the `noleme-flow-connectors` ecosystem. This chapter explores how to use these connectors to build real-world ETL pipelines.

## The Connector Ecosystem

The `noleme-flow-connectors` project is a collection of libraries providing specialized `Extractor`, `Transformer`, and `Loader` implementations.

| Library            | Description                                                                               |
|--------------------|-------------------------------------------------------------------------------------------|
| `connect-commons`  | File system access, JSON/YAML parsing, and collection generators.                         |
| `connect-http`     | HTTP client integrations (based on `java.net.http.HttpClient`).                           |
| `connect-tablesaw` | Integration with the [Tablesaw](https://github.com/jtablesaw/tablesaw) dataframe library. |
| `connect-jsoup`    | HTML parsing and web scraping using JSoup.                                                |
| `connect-kafka`    | Kafka consumers and producers.                                                            |
| `connect-aws`      | S3 and other AWS service integrations.                                                    |

## Practical Example: A Web-to-CSV ETL

Imagine we need to fetch a JSON report from an HTTP API, filter the data using a dataframe library, and save the result as a CSV file.

### 1. Setup

First, ensure you have the necessary dependencies (e.g., `noleme-flow-connect-http` and `noleme-flow-connect-tablesaw`).

### 2. Building the Flow

```java
var flow = Flow
    /* 1. Extract: Fetch raw JSON from an API */
    .from(new HttpGetter())
    
    /* 2. Transform: Parse JSON into a Tablesaw Table */
    .pipe(new TablesawJSONParser())
    
    /* 3. Transform: Apply domain logic using Tablesaw's fluent API */
    .pipe(table -> table.where(table.numberColumn("revenue").isGreaterThan(1000)))
    
    /* 4. Load: Write the filtered data to a local CSV file */
    .sink(new TablesawCSVWriter("output/high_revenue_report.csv"));

Flow.runAsPipeline(flow);
```

## Reusability with Flow Fragments

One of the key design goals of `noleme-flow` is the ability to compose larger pipelines from smaller, reusable fragments.

### Defining a Fragment

You can define a reusable sequence of operations as a `PipeSlice`.

```java
public class AnalyticsSlice extends PipeSlice<Table, Table> {
    @Override
    public CurrentOut<Table> out(CurrentOut<Table> upstream) {
        return upstream
            .pipe(table -> table.summarize("revenue", sum).by("category"))
            .pipe(table -> table.sortDescendingOn("sum [revenue]"));
    }
}
```

### Using the Fragment

```java
var flow = Flow
    .from(() -> new TablesawCSVReader().extract())
    .pipe(new AnalyticsSlice()) // Reusable logic
    .sink(new TablesawCSVWriter("data/summary.csv"));
```

## Error Handling in Practice

In real-world ETLs, external systems (APIs, Databases) can fail. Use `nonFatal` to prevent a single failing record or source from stopping the entire pipeline.

```java
var flow = Flow
    .from(Flow.nonFatal(new HttpGetter(), (exception) -> {
        logger.error("Failed to fetch data", exception);
    }))
    .pipe(...)
    .sink(...);
```

By using `nonFatal`, if the `HttpGetter` fails, the downstream branch for that specific input will be skipped, but the rest of the flow (if it were part of a stream or had other sources) would continue.

## Summary

`noleme-flow` combined with its connectors provides a lightweight yet robust framework for data engineering. Whether you are doing simple file transformations or complex multi-source joins, the composition model ensures your logic remains modular and testable.
