package com.noleme.flow.impl.parallel;

import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.compiler.FlowCompiler;
import com.noleme.flow.impl.parallel.compiler.ParallelIndexes;
import com.noleme.flow.impl.parallel.compiler.pass.RemoveNodesWithUpstreamPass;
import com.noleme.flow.impl.parallel.runtime.executor.ExecutorServiceProvider;
import com.noleme.flow.impl.parallel.runtime.executor.Executors;
import com.noleme.flow.impl.pipeline.PipelineCompiler;
import com.noleme.flow.impl.pipeline.compiler.pass.PipelineCompilerPass;
import com.noleme.flow.impl.pipeline.compiler.pass.TopologicalSortPass;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamGenerator;
import com.noleme.flow.stream.StreamNode;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
@SuppressWarnings("rawtypes")
public class ParallelCompiler implements FlowCompiler<ParallelRuntime>
{
    private final ExecutorServiceProvider provider;
    private final boolean autoRefresh;
    private final List<PipelineCompilerPass> passes = List.of(
        new TopologicalSortPass(),
        new RemoveNodesWithUpstreamPass()
    );

    /**
     *
     */
    public ParallelCompiler()
    {
        this(Runtime.getRuntime().availableProcessors(), true);
    }

    /**
     *
     * @param threadCount
     * @param autoRefresh
     */
    public ParallelCompiler(int threadCount, boolean autoRefresh)
    {
        this(() -> Executors.newFixedThreadPool(threadCount), autoRefresh);
    }

    /**
     *
     * @param provider
     * @param autoRefresh
     */
    public ParallelCompiler(ExecutorServiceProvider provider, boolean autoRefresh)
    {
        this.provider = provider;
        this.autoRefresh = autoRefresh;
    }

    @Override
    public ParallelRuntime compile(Collection<Node> inputNodes) throws CompilationException
    {
        List<Node> compiledNodes = PipelineCompiler.compile(inputNodes, this.passes);
        ParallelIndexes indexes = this.computeIndexes(compiledNodes);

        return new ParallelRuntime(
            compiledNodes,
            this.provider,
            this.autoRefresh,
            indexes
        );
    }

    /* Index-related actions */

    private ParallelIndexes computeIndexes(List<Node> compiledNodes)
    {
        Map<Node, StreamGenerator> generatorIndex = new HashMap<>();
        Map<StreamGenerator, Set<Node>> streamNodeIndex = new HashMap<>();

        this.indexStreamNodes(compiledNodes, generatorIndex, streamNodeIndex);

        return new ParallelIndexes(
            generatorIndex,
            streamNodeIndex
        );
    }

    /**
     *
     * @param nodes
     * @param generatorIndex
     * @param nodeIndex
     */
    private void indexStreamNodes(List<Node> nodes, Map<Node, StreamGenerator> generatorIndex, Map<StreamGenerator, Set<Node>> nodeIndex)
    {
        for (Node node : nodes)
        {
            if (node instanceof StreamGenerator)
            {
                if (!nodeIndex.containsKey(node))
                    nodeIndex.put((StreamGenerator) node, new HashSet<>());
                indexStreamNodes(node.getDownstream(), (StreamGenerator) node, generatorIndex, nodeIndex);
            }
            else
                indexStreamNodes(node.getDownstream(), generatorIndex, nodeIndex);
        }
    }

    /**
     *
     * @param nodes
     * @param generator
     * @param generatorIndex
     * @param nodeIndex
     */
    private void indexStreamNodes(List<Node> nodes, StreamGenerator generator, Map<Node, StreamGenerator> generatorIndex, Map<StreamGenerator, Set<Node>> nodeIndex)
    {
        for (Node node : nodes)
        {
            if (node instanceof StreamNode)
            {
                if (!generatorIndex.containsKey(node))
                {
                    generatorIndex.put(node, generator);
                    indexStreamNodes(node.getDownstream(), generator, generatorIndex, nodeIndex);
                }
                nodeIndex.get(generator).add(node);
            }
            else
                indexStreamNodes(node.getDownstream(), generatorIndex, nodeIndex);
        }
    }
}
