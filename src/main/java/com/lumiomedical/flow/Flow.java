package com.lumiomedical.flow;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.FlowCompiler;
import com.lumiomedical.flow.compiler.FlowRuntime;
import com.lumiomedical.flow.compiler.RunException;
import com.lumiomedical.flow.compiler.pipeline.ParallelPipelineCompiler;
import com.lumiomedical.flow.compiler.pipeline.ParallelPipelineRuntime;
import com.lumiomedical.flow.compiler.pipeline.PipelineCompiler;
import com.lumiomedical.flow.compiler.pipeline.PipelineRuntime;
import com.lumiomedical.flow.compiler.pipeline.parallel.ExecutorServiceProvider;
import com.lumiomedical.flow.etl.extractor.Extractor;
import com.lumiomedical.flow.etl.transformer.BiTransformer;
import com.lumiomedical.flow.node.Node;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

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
     * @return the resulting FlowRuntime instance
     * @throws CompilationException
     * @throws RunException
     */
    public static <C extends FlowCompiler<R>, R extends FlowRuntime> R runAs(C compiler, Node... inputNodes) throws CompilationException, RunException
    {
        R runtime = compiler.compile(inputNodes);
        runtime.run();
        return runtime;
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the PipelineCompiler implementation.
     *
     * @param inputNodes one or several nodes from the DAG
     * @return the resulting PipelineRuntime instance
     * @throws CompilationException
     * @throws RunException
     */
    public static PipelineRuntime runAsPipeline(Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new PipelineCompiler(), inputNodes);
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the ParallelPipelineCompiler implementation.
     * This implementation uses a ThrowingThreadPoolExecutor implementation as an ExecutorService.
     * Property autoRefresh is set to true, so the ExecutorService will be shutdown/relaunched between each run.
     *
     * @param threadCount the number of threads used for parallelization
     * @param inputNodes one or several nodes from the DAG
     * @return the resulting ParallelPipelineRuntime instance
     * @throws CompilationException
     * @throws RunException
     */
    public static ParallelPipelineRuntime runAsParallelPipeline(int threadCount, Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelPipelineCompiler(threadCount, true), inputNodes);
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the ParallelPipelineCompiler implementation.
     *
     * @param provider an ExecutorServiceProvider for setting up the ExecutorService used for parallelization
     * @param autoRefresh whether to automatically shutdown the ExecutorService upon running, or preserve it for subsequent runs
     * @param inputNodes one or several nodes from the DAG
     * @return the resulting ParallelPipelineRuntime instance
     * @throws CompilationException
     * @throws RunException
     */
    public static ParallelPipelineRuntime runAsParallelPipeline(ExecutorServiceProvider provider, boolean autoRefresh, Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelPipelineCompiler(provider, autoRefresh), inputNodes);
    }

    /**
     * Compiles and runs the provided DAG as a pipeline using the ParallelPipelineCompiler implementation.
     * This implementation uses a ThrowingThreadPoolExecutor implementation as an ExecutorService.
     * The number of threads used for parallelization is equal to Runtime.getRuntime().availableProcessors().
     * Property autoRefresh is set to true, so the ExecutorService will be shutdown/relaunched between each run.
     *
     * @param inputNodes one or several nodes from the DAG
     * @return the resulting ParallelPipelineRuntime instance
     * @throws CompilationException
     * @throws RunException
     */
    public static ParallelPipelineRuntime runAsParallelPipeline(Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelPipelineCompiler(), inputNodes);
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
        return new Join<>(input1, input2, transformer);
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
