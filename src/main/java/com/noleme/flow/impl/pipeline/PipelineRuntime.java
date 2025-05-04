package com.noleme.flow.impl.pipeline;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.compiler.FlowRuntime;
import com.noleme.flow.compiler.RunException;
import com.noleme.flow.impl.pipeline.compiler.stream.StreamPipeline;
import com.noleme.flow.impl.pipeline.runtime.execution.Execution;
import com.noleme.flow.impl.pipeline.runtime.heap.HashHeap;
import com.noleme.flow.impl.pipeline.runtime.heap.Heap;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingKey;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingNode;
import com.noleme.flow.io.input.Input;
import com.noleme.flow.io.output.Output;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamAccumulator;
import com.noleme.flow.stream.StreamGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public class PipelineRuntime implements FlowRuntime
{
    protected final Execution execution;
    private final List<Node> compiledNodes;

    private static final Logger logger = LoggerFactory.getLogger(PipelineRuntime.class);

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
    @SuppressWarnings("unchecked")
    public Output run(Input input) throws RunException
    {
        LinkedList<WorkingNode<?>> runQueue = this.compiledNodes.stream()
            .map(WorkingNode::new)
            .collect(Collectors.toCollection(LinkedList::new))
        ;
        Set<WorkingKey> blocked = new HashSet<>();
        var heap = new HashHeap(input);

        try {
            heap.getOutput().setStartTime(Instant.now());

            /*
             * Fires the whole running queue and discards dead branches resulting from failed executions.
             * Upon a successful run, the outbounds that haven't been added yet are pushed to the waiting queue.
             */
            while (!runQueue.isEmpty())
            {
                WorkingNode<?> n = runQueue.poll();

                if (blocked.contains(n.getKey()))
                    continue;

                /* If the node is a StreamPipelineNode we need to register a stream round */
                if (n.getNode() instanceof StreamPipeline)
                    registerStream((WorkingNode<StreamPipeline>) n, runQueue, heap);
                /* Otherwise we handle it as a standard node */
                else if (!this.execution.launch(n, heap))
                    blockBranch(n, blocked);
            }

            return heap.getOutput();
        }
        finally {
            heap.getOutput().setEndTime(Instant.now());
            logger.info("Ran pipeline in {}", heap.getOutput().elapsedTimeString());
        }
    }

    /**
     *
     * @param n
     * @param blocked
     */
    public static void blockBranch(WorkingNode<?> n, Set<WorkingKey> blocked)
    {
        Queue<WorkingNode<?>> q = new LinkedList<>();
        q.add(n);
        while (!q.isEmpty())
        {
            WorkingNode<?> wn = q.poll();
            Node node = wn.getNode();

            /* We don't block stream accumulators as they are expected to accumulate any stream that did complete, and return an empty list if none did */
            if (node instanceof StreamAccumulator)
                continue;

            blocked.add(wn.getKey());
            q.addAll(wn.getWorkingDownstream());
        }
    }

    /**
     *
     * @param pipelineNode
     * @param runQueue
     * @param heap
     */
    @SuppressWarnings("rawtypes")
    private static void registerStream(WorkingNode<StreamPipeline> pipelineNode, LinkedList<WorkingNode<?>> runQueue, Heap heap)
    {
        StreamPipeline pipeline = pipelineNode.getNode();
        StreamGenerator generatorNode = pipeline.getGeneratorNode();

        //FIXME: likely shenanigans going on here
        WorkingKey parentKey = pipelineNode.getKey().hasOffset() ? pipelineNode.getKey() : null;

        WorkingNode<StreamGenerator> workingGeneratorNode = new WorkingNode<>(generatorNode, pipelineNode.getKey().offset(), parentKey);
        Generator<?> generator = heap.getStreamGenerator(workingGeneratorNode);

        if (generator.hasNext())
        {
            long offset = heap.getNextStreamOffset(workingGeneratorNode);

            /* We add the stream pipeline to the top of the queue, in case it will still have to iterate further */
            runQueue.push(pipelineNode);

            /* Add stream nodes to the top of the queue, note that we need to do in reverse order as we push to the top */
            var reverseIterator = pipeline.getNodes().listIterator(pipeline.getNodes().size());
            while (reverseIterator.hasPrevious())
            {
                Node streamNode = reverseIterator.previous();
                if (streamNode instanceof StreamAccumulator)
                    continue;

                runQueue.push(new WorkingNode<>(streamNode, offset, parentKey));
            }

            /* We add the generator to the top of the queue so it can generate the input required by previously added stream nodes */
            runQueue.push(new WorkingNode<>(generatorNode, offset, parentKey));
        }
        else {
            pipeline.getNodes().stream()
                .filter(n -> n instanceof StreamAccumulator)
                .map(n -> (StreamAccumulator) n)
                .forEach(a -> runQueue.push(new WorkingNode<>(a, parentKey)))
            ;
        }
    }
}
