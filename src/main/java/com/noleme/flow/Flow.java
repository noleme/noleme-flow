package com.noleme.flow;

import com.noleme.flow.actor.extractor.ExtractionException;
import com.noleme.flow.actor.extractor.Extractor;
import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.actor.loader.Loader;
import com.noleme.flow.actor.transformer.BiTransformer;
import com.noleme.flow.actor.transformer.TransformationException;
import com.noleme.flow.actor.transformer.Transformer;
import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.compiler.FlowCompiler;
import com.noleme.flow.compiler.FlowRuntime;
import com.noleme.flow.compiler.RunException;
import com.noleme.flow.impl.parallel.ParallelCompiler;
import com.noleme.flow.impl.parallel.runtime.executor.ExecutorServiceProvider;
import com.noleme.flow.impl.pipeline.PipelineCompiler;
import com.noleme.flow.interruption.InterruptionException;
import com.noleme.flow.io.input.Input;
import com.noleme.flow.io.input.InputExtractor;
import com.noleme.flow.io.output.Output;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A helper class for initiating flows, it also provides a handful of shorthand methods for building and launching flow DAGs.
 *
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/28
 */
public final class Flow
{
    private static final Logger logger = LoggerFactory.getLogger(Flow.class);

    private Flow() {}

    /**
     * Compiles and runs the provided DAG using the provided FlowCompiler implementation.
     *
     * @param compiler a FlowCompiler instance
     * @param inputNodes one or several nodes from the DAG
     * @return the Output result
     * @throws CompilationException
     * @throws RunException
     */
    public static <C extends FlowCompiler<R>, R extends FlowRuntime> Output runAs(C compiler, Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(compiler, Input.empty(), inputNodes);
    }

    /**
     * Compiles and runs the provided DAG using the provided FlowCompiler implementation.
     *
     * @param compiler a FlowCompiler instance
     * @param input an Input instance
     * @param inputNodes one or several nodes from the DAG
     * @return the Output result
     * @throws CompilationException
     * @throws RunException
     */
    public static <C extends FlowCompiler<R>, R extends FlowRuntime> Output runAs(C compiler, Input input, Node... inputNodes) throws CompilationException, RunException
    {
        R runtime = compiler.compile(inputNodes);
        return runtime.run(input);
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the PipelineCompiler implementation.
     *
     * @param inputNodes one or several nodes from the DAG
     * @return the resulting PipelineRuntime instance
     * @throws CompilationException
     * @throws RunException
     */
    public static Output runAsPipeline(Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new PipelineCompiler(), inputNodes);
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the PipelineCompiler implementation.
     *
     * @param input an input container for the DAG at runtime
     * @param inputNodes one or several nodes from the DAG
     * @return the Output result
     * @throws CompilationException
     * @throws RunException
     */
    public static Output runAsPipeline(Input input, Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new PipelineCompiler(), input, inputNodes);
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the ParallelPipelineCompiler implementation.
     * This implementation uses a ThrowingThreadPoolExecutor implementation as an ExecutorService.
     * Property autoRefresh is set to true, so the ExecutorService will be shutdown/relaunched between each run.
     *
     * @param threadCount the number of threads used for parallelization
     * @param inputNodes one or several nodes from the DAG
     * @return the Output result
     * @throws CompilationException
     * @throws RunException
     */
    public static Output runAsParallel(int threadCount, Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelCompiler(threadCount, true), inputNodes);
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the ParallelPipelineCompiler implementation.
     * Property autoRefresh is set to true, so the ExecutorService will be shutdown/relaunched between each run.
     *
     * @param provider an ExecutorServiceProvider for setting up the ExecutorService used for parallelization
     * @param input an input container for the DAG at runtime
     * @param inputNodes one or several nodes from the DAG
     * @return the Output result
     * @throws CompilationException
     * @throws RunException
     */
    public static Output runAsParallel(ExecutorServiceProvider provider, Input input, Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelCompiler(provider, true), input, inputNodes);
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the ParallelPipelineCompiler implementation.
     * This implementation uses a ThrowingThreadPoolExecutor implementation as an ExecutorService.
     * The number of threads used for parallelization is equal to Runtime.getRuntime().availableProcessors().
     * Property autoRefresh is set to true, so the ExecutorService will be shutdown/relaunched between each run.
     *
     * @param inputNodes one or several nodes from the DAG
     * @return the Output result
     * @throws CompilationException
     * @throws RunException
     */
    public static Output runAsParallel(Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelCompiler(), inputNodes);
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the ParallelPipelineCompiler implementation.
     * This implementation uses a ThrowingThreadPoolExecutor implementation as an ExecutorService.
     * The number of threads used for parallelization is equal to Runtime.getRuntime().availableProcessors().
     * Property autoRefresh is set to true, so the ExecutorService will be shutdown/relaunched between each run.
     *
     * @param input an input container for the DAG at runtime
     * @param inputNodes one or several nodes from the DAG
     * @return the Output result
     * @throws CompilationException
     * @throws RunException
     */
    public static Output runAsParallel(Input input, Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelCompiler(), input, inputNodes);
    }

    /**
     * Returns a Source node from a given Extractor instance.
     * The source node can be used to initiate a flow.
     *
     * @param extractor an Extractor instance from which to initiate a flow Source
     * @param <O> the type of the Source
     * @return the resulting Source node
     */
    public static <O> Source<O> from(Extractor<O> extractor)
    {
        return new Source<>(extractor);
    }

    /**
     *
     * @param transformer
     * @param input
     * @param <I>
     * @param <O>
     * @return
     */
    public static <I, O> Source<O> from(Transformer<I, O> transformer, I input)
    {
        return new Source<>(transformer.asExtractor(input));
    }

    /**
     *
     * @param inputIdentifier
     * @param <O>
     * @return
     */
    public static <O> Source<O> from(String inputIdentifier)
    {
        return new Source<>(new InputExtractor<>(inputIdentifier));
    }

    /**
     *
     * @param generatorSupplier
     * @param <O>
     * @return
     */
    public static <O> StreamGenerator<Void, O> stream(Supplier<Generator<O>> generatorSupplier)
    {
        return new StreamGenerator<>(i -> generatorSupplier.get());
    }

    /** @see #pipe(FlowOut, Transformer) */
    public static <I, O> Pipe<I, O> into(FlowOut<I> flow, Transformer<I, O> transformer)
    {
        return flow.into(transformer);
    }

    /** @see #sink(FlowOut, Loader) */
    public static <I> Sink<I> into(FlowOut<I> flow, Loader<I> loader)
    {
        return flow.into(loader);
    }

    /** @see #stream(FlowOut, Function) */
    public static <I, O> StreamGenerator<I, O> into(FlowOut<I> flow, Function<I, Generator<O>> generatorSupplier)
    {
        return flow.stream(generatorSupplier);
    }

    /** @see #pipe(StreamOut, Transformer) */
    public static <I, O> StreamPipe<I, O> into(StreamOut<I> flow, Transformer<I, O> transformer)
    {
        return flow.into(transformer);
    }

    /** @see #sink(StreamOut, Loader) */
    public static <I> StreamSink<I> into(StreamOut<I> flow, Loader<I> loader)
    {
        return flow.into(loader);
    }

    /**
     * Returns the Pipe node resulting from the binding of a provided flow to a Transformer.
     *
     * @param flow a flow to which the Transformer will be bound
     * @param transformer the Transformer to bind
     * @param <I> the type of the upstream flow
     * @param <O> the type of the downstream flow
     * @return the resulting Pipe node
     */
    public static <I, O> Pipe<I, O> pipe(FlowOut<I> flow, Transformer<I, O> transformer)
    {
        return flow.into(transformer);
    }

    /**
     * Returns the Sink node resulting from the binding of a provided flow to a Loader.
     *
     * @param flow a flow to which the Loader will be bound
     * @param loader the Loader to bind
     * @param <I> the type of the upstream flow
     * @return the resulting Sink node
     */
    public static <I> Sink<I> sink(FlowOut<I> flow, Loader<I> loader)
    {
        return flow.into(loader);
    }

    /**
     * Returns the StreamPipe node resulting from the binding of a provided stream flow to a Transformer.
     *
     * @param flow a flow to which the Transformer will be bound
     * @param transformer the Transformer to bind
     * @param <I> the type of the upstream flow
     * @param <O> the type of the downstream flow
     * @return the resulting StreamPipe node
     */
    public static <I, O> StreamPipe<I, O> pipe(StreamOut<I> flow, Transformer<I, O> transformer)
    {
        return flow.into(transformer);
    }

    /**
     * Returns the StreamSink node resulting from the binding of a provided stream flow to a Loader.
     *
     * @param flow a flow to which the Loader will be bound
     * @param loader the Loader to bind
     * @param <I> the type of the upstream flow
     * @return the resulting StreamSink node
     */
    public static <I> StreamSink<I> sink(StreamOut<I> flow, Loader<I> loader)
    {
        return flow.into(loader);
    }

    /**
     * Returns a Join node resulting from the joining of two flows using a provided BiTransformer.
     *
     * @param input1 a FlowOut node of an incoming flow A
     * @param input2 a FlowOut node of an incoming flow B
     * @param transformer a BiTransformer describing how to perform the join
     * @param <I1> the type of incoming flow A
     * @param <I2> the type of incoming flow B
     * @param <O> the type resulting from the merging of flow A and B
     * @return the resulting Join node
     */
    public static <I1, I2, O> Join<I1, I2, O> join(FlowOut<I1> input1, FlowOut<I2> input2, BiTransformer<I1, I2, O> transformer)
    {
        return input1.join(input2, transformer);
    }

    /**
     *
     * @param flow a flow from which to initiate the stream
     * @param generatorSupplier
     * @param <I>
     * @param <O>
     * @return
     */
    public static <I, O> StreamGenerator<I, O> stream(FlowOut<I> flow, Function<I, Generator<O>> generatorSupplier)
    {
        return flow.stream(generatorSupplier);
    }

    /**
     * Returns a StreamJoin node resulting from the joining of a stream flow and a non-stream flow using a provided BiTransformer.
     *
     * @param input1 a StreamOut node of an incoming stream flow A
     * @param input2 a FlowOut node of an incoming flow B
     * @param transformer a BiTransformer describing how to perform the join
     * @param <I1> the type of incoming flow A
     * @param <I2> the type of incoming flow B
     * @param <O> the type resulting from the merging of flow A and B
     * @return the resulting StreamJoin node
     */
    public static <I1, I2, O> StreamJoin<I1, I2, O> join(StreamOut<I1> input1, FlowOut<I2> input2, BiTransformer<I1, I2, O> transformer)
    {
        return input1.join(input2, transformer);
    }

    /**
     * An adapter function for absorbing ExtractionException and replace them with a log line and the control InterruptionException.
     *
     * @param extractor An extractor instance to be wrapped
     * @param <O> the type of the downstream flow
     * @return the resulting wrapper Extractor node
     */
    public static <O> Extractor<O> nonFatal(Extractor<O> extractor)
    {
        return () -> {
            try {
                return extractor.extract();
            }
            catch (ExtractionException e) {
                logger.error(e.getMessage(), e);
                throw InterruptionException.interrupt();
            }
        };
    }

    /**
     * An adapter function for absorbing TransformationException and replace them with a log line and the control InterruptionException.
     *
     * @param transformer A transformer instance to be wrapped
     * @param <I> the type of the upstream flow
     * @param <O> the type of the downstream flow
     * @return the resulting wrapper Transformer node
     */
    public static <I, O> Transformer<I, O> nonFatal(Transformer<I, O> transformer)
    {
        return input -> {
            try {
                return transformer.transform(input);
            }
            catch (TransformationException e) {
                logger.error(e.getMessage(), e);
                throw InterruptionException.interrupt();
            }
        };
    }

    /**
     * An adapter function for absorbing TransformationException and replace them with a log line and the control InterruptionException.
     *
     * @param transformer A bi-transformer instance to be wrapped
     * @param <I1> the type of incoming flow A
     * @param <I2> the type of incoming flow B
     * @param <O> the type of the downstream flow
     * @return the resulting wrapper BiTransformer node
     */
    public static <I1, I2, O> BiTransformer<I1, I2, O> nonFatal(BiTransformer<I1, I2, O> transformer)
    {
        return (a, b) -> {
            try {
                return transformer.transform(a, b);
            }
            catch (TransformationException e) {
                logger.error(e.getMessage(), e);
                throw InterruptionException.interrupt();
            }
        };
    }

    /**
     * An adapter function for leveraging a given Transformer over a collection of its inputs.
     * It essentially produces a Transformer that will iterate over the input collection and delegate each item to the provided Transformer implementation.
     *
     * @param transformer A transformer instance to be wrapped
     * @param <C> the type of the upstream flow collection
     * @param <I> the type of the upstream flow collection items
     * @param <O> the type of the downstream flow
     * @return the resulting wrapper Transformer node
     */
    public static <I, O, C extends Collection<I>> Transformer<C, List<O>> aggregate(Transformer<I, O> transformer)
    {
        return collection -> {
            List<O> output = new ArrayList<>(collection.size());
            for (I input : collection)
                output.add(transformer.transform(input));
            return output;
        };
    }

    /**
     * An adapter function for leveraging a given BiTransformer over a collection of its inputs.
     * It essentially produces a BiTransformer that will iterate over the input collection and delegate each item to the provided BiTransformer implementation.
     *
     * @param transformer A bi-transformer instance to be wrapped
     * @param <C> the type of incoming flow collection A
     * @param <I1> the type of incoming flow collection items A
     * @param <I2> the type of incoming flow B
     * @param <O> the type of the downstream flow
     * @return the resulting wrapper BiTransformer node
     */
    public static <I1, I2, O, C extends Collection<I1>> BiTransformer<C, I2, List<O>> aggregate(BiTransformer<I1, I2, O> transformer)
    {
        return (collection, joined) -> {
            List<O> output = new ArrayList<>(collection.size());
            for (I1 input : collection)
                output.add(transformer.transform(input, joined));
            return output;
        };
    }

    /**
     * An adapter function for leveraging a given Loader over a collection of its inputs.
     * It essentially produces a Loader that will iterate over the input collection and delegate each item to the provided Loader implementation.
     *
     * @param loader A loader instance to be wrapped
     * @param <C> the type of the upstream flow collection
     * @param <I> the type of the upstream flow collection items
     * @return the resulting wrapper Loader node
     */
    public static <I, C extends Collection<I>> Loader<C> aggregate(Loader<I> loader)
    {
        return collection -> {
            for (I input : collection)
                loader.load(input);
        };
    }

    /**
     * Returns a set of Nodes that are considered to be "source nodes" or "root nodes" to the provided node.
     * A "source node" is defined here as an entry-point into the node graph that has to be traversed in order to reach a given node.
     *
     * This can be useful if you want to chain pipeline sections for which you only have references to the lowest nodes (which should be common given the builder pattern used by the Pipe API).
     *
     * @param node a node from which to perform the Source lookup
     * @return a set containing any Source node that was found to be a upstream of the provided node
     */
    public static Set<Node> sources(Node node)
    {
        Set<Node> roots = new HashSet<>();
        Set<Node> visited = new HashSet<>();
        Queue<Node> queue = new LinkedList<>();

        queue.add(node);
        while (!queue.isEmpty())
        {
            Node n = queue.poll();
            visited.add(n);

            if (n.getUpstream().isEmpty())
                roots.add(n);

            for (Node usn : n.getUpstream())
            {
                if (!visited.contains(usn))
                    queue.add(usn);
            }
        }

        return roots;
    }
}