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
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/28
 */
public final class Flow
{
    private Flow()
    {
    }

    /**
     *
     * @param compiler
     * @param inputNodes
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
     *
     * @param inputNodes
     * @throws CompilationException
     * @throws RunException
     */
    public static PipelineRuntime runAsPipeline(Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new PipelineCompiler(), inputNodes);
    }

    /**
     *
     * @param threadCount
     * @param inputNodes
     * @throws CompilationException
     * @throws RunException
     */
    public static ParallelPipelineRuntime runAsParallelPipeline(int threadCount, Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelPipelineCompiler(threadCount, true), inputNodes);
    }

    /**
     *
     * @param provider
     * @param autoRefresh
     * @param inputNodes
     * @throws CompilationException
     * @throws RunException
     */
    public static ParallelPipelineRuntime runAsParallelPipeline(ExecutorServiceProvider provider, boolean autoRefresh, Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelPipelineCompiler(provider, autoRefresh), inputNodes);
    }

    /**
     *
     * @param inputNodes
     * @throws CompilationException
     * @throws RunException
     */
    public static ParallelPipelineRuntime runAsParallelPipeline(Node... inputNodes) throws CompilationException, RunException
    {
        return runAs(new ParallelPipelineCompiler(), inputNodes);
    }

    /**
     * Returns a source node from a given Extractor instance.
     * The source node can be used to initiate a flow.
     *
     * @param extractor
     * @param <O>
     * @return
     */
    public static <O> Source<O> from(Extractor<O> extractor)
    {
        return new Source<>(extractor);
    }

    /**
     *
     * @param input1
     * @param input2
     * @param transformer
     * @param <I1>
     * @param <I2>
     * @param <O>
     * @return
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
     * @param node
     * @return
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
