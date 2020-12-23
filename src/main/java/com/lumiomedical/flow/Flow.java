package com.lumiomedical.flow;

import com.lumiomedical.flow.actor.extractor.Extractor;
import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.actor.loader.Loader;
import com.lumiomedical.flow.actor.transformer.BiTransformer;
import com.lumiomedical.flow.actor.transformer.Transformer;
import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.FlowCompiler;
import com.lumiomedical.flow.compiler.FlowRuntime;
import com.lumiomedical.flow.compiler.RunException;
import com.lumiomedical.flow.impl.parallel.ParallelCompiler;
import com.lumiomedical.flow.impl.parallel.runtime.executor.ExecutorServiceProvider;
import com.lumiomedical.flow.impl.pipeline.PipelineCompiler;
import com.lumiomedical.flow.io.input.Input;
import com.lumiomedical.flow.io.input.InputExtractor;
import com.lumiomedical.flow.io.output.Output;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.stream.*;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
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
