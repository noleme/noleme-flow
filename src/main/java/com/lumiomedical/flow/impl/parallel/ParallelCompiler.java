package com.lumiomedical.flow.impl.parallel;

import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.FlowCompiler;
import com.lumiomedical.flow.impl.parallel.compiler.ParallelIndexes;
import com.lumiomedical.flow.impl.parallel.compiler.pass.RemoveNodesWithUpstreamPass;
import com.lumiomedical.flow.impl.parallel.runtime.executor.ExecutorServiceProvider;
import com.lumiomedical.flow.impl.parallel.runtime.executor.Executors;
import com.lumiomedical.flow.impl.pipeline.PipelineCompiler;
import com.lumiomedical.flow.impl.pipeline.compiler.pass.PipelineCompilerPass;
import com.lumiomedical.flow.impl.pipeline.compiler.pass.TopologicalSortPass;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.recipient.Recipient;
import com.lumiomedical.flow.stream.StreamGenerator;
import com.lumiomedical.flow.stream.StreamNode;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
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
        Map<String, Recipient> recipients = new HashMap<>();
        Map<Node, StreamGenerator> generatorIndex = new HashMap<>();
        Map<StreamGenerator, Set<Node>> streamNodeIndex = new HashMap<>();

        this.indexRecipients(compiledNodes, recipients);
        this.indexStreamNodes(compiledNodes, generatorIndex, streamNodeIndex);

        return new ParallelIndexes(
            recipients,
            generatorIndex,
            streamNodeIndex
        );
    }

    /**
     *
     * @param compiledNodes
     * @return
     */
    private void indexRecipients(List<Node> compiledNodes, Map<String, Recipient> recipients)
    {
        Queue<Node> queue = new LinkedList<>(compiledNodes);

        while (!queue.isEmpty())
        {
            Node node = queue.poll();

            if (node instanceof Recipient)
            {
                var recipient = (Recipient) node;
                recipients.put(recipient.getName(), recipient);
            }

            queue.addAll(node.getDownstream());
        }
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
