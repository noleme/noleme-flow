package com.noleme.flow.impl.pipeline;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.compiler.FlowRuntime;
import com.noleme.flow.compiler.RunException;
import com.noleme.flow.impl.pipeline.compiler.stream.StreamPipeline;
import com.noleme.flow.impl.pipeline.runtime.execution.Execution;
import com.noleme.flow.impl.pipeline.runtime.heap.HashHeap;
import com.noleme.flow.impl.pipeline.runtime.heap.Heap;
import com.noleme.flow.impl.pipeline.runtime.node.OffsetNode;
import com.noleme.flow.io.input.Input;
import com.noleme.flow.io.output.Output;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamAccumulator;
import com.noleme.flow.stream.StreamGenerator;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public class PipelineRuntime implements FlowRuntime
{
    protected final Execution execution;
    private final List<Node> compiledNodes;

    /**
     *
     * @param compiledNodes
     */
    protected PipelineRuntime(List<Node> compiledNodes)
    {
        this.execution = new Execution();
        this.compiledNodes = compiledNodes;
    }

    @Override
    public Output run(Input input) throws RunException
    {
        LinkedList<Node> runQueue = new LinkedList<>(this.compiledNodes);
        Set<Node> blocked = new HashSet<>();
        Heap heap = new HashHeap(input);

        /*
         * Fires the whole running queue and discards dead branches resulting from failed executions.
         * Upon a successful run, the outbounds that haven't been added yet are pushed to the waiting queue.
         */
        while (!runQueue.isEmpty())
        {
            Node n = runQueue.poll();

            if (blocked.contains(n))
                continue;

            /* If the node is a StreamPipelineNode we need to register a stream round */
            if (n instanceof StreamPipeline)
                registerStream((StreamPipeline) n, runQueue, heap);
            /* Otherwise we handle it as a standard node */
            else if (!this.execution.launch(n, heap))
                blockBranch(n, blocked);
        }

        return heap.getOutput();
    }

    /**
     *
     * @param n
     * @param blocked
     */
    public static void blockBranch(Node n, Set<Node> blocked)
    {
        Queue<Node> q = new LinkedList<>();
        q.add(n);
        while (!q.isEmpty())
        {
            Node node = q.poll();

            /* We don't block stream accumulators as they are expected to accumulate any stream that did complete, and return an empty list if none did */
            if (node instanceof StreamAccumulator)
                continue;

            blocked.add(node);
            q.addAll(node.getDownstream());
        }
    }

    /**
     *
     * @param node
     * @param runQueue
     * @param heap
     */
    private static void registerStream(StreamPipeline node, LinkedList<Node> runQueue, Heap heap)
    {
        StreamGenerator generatorNode = node.getGeneratorNode();
        Generator generator = heap.getStreamGenerator(generatorNode);

        if (generator.hasNext())
        {
            int offset = heap.getNextStreamOffset(generatorNode);

            /* We add the stream pipeline to the top of the queue, in case it will still have to iterate further */
            runQueue.push(node);

            /* Add stream nodes to the top of the queue, note that we need to do in reverse order as we push to the top */
            var reverseIterator = node.getNodes().listIterator(node.getNodes().size());
            while (reverseIterator.hasPrevious())
            {
                Node streamNode = reverseIterator.previous();
                runQueue.push(new OffsetNode(streamNode, offset));
            }

            /* We add the generator to the top of the queue so it can generate the input required by previously added stream nodes */
            runQueue.push(new OffsetNode(generatorNode, offset));
        }
    }
}
