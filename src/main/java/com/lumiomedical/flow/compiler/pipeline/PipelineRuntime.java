package com.lumiomedical.flow.compiler.pipeline;

import com.lumiomedical.flow.actor.generator.GenerationException;
import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.compiler.FlowRuntime;
import com.lumiomedical.flow.compiler.RunException;
import com.lumiomedical.flow.compiler.pipeline.execution.Execution;
import com.lumiomedical.flow.compiler.pipeline.heap.HashHeap;
import com.lumiomedical.flow.compiler.pipeline.heap.Heap;
import com.lumiomedical.flow.compiler.pipeline.stream.StreamHashHeap;
import com.lumiomedical.flow.compiler.pipeline.stream.StreamHeap;
import com.lumiomedical.flow.compiler.pipeline.stream.StreamPipelineNode;
import com.lumiomedical.flow.logger.Logging;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.recipient.Recipient;
import com.lumiomedical.flow.stream.StreamAccumulator;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public class PipelineRuntime implements FlowRuntime
{
    protected final Execution execution;
    private final List<Node> compiledNodes;
    private final Map<String, Recipient> recipients;

    /**
     *
     * @param compiledNodes
     */
    public PipelineRuntime(List<Node> compiledNodes)
    {
        this.execution = new Execution();
        this.compiledNodes = compiledNodes;
        this.recipients = this.identifyRecipientNodes(compiledNodes);
    }

    @Override
    public void run() throws RunException
    {
        this.run(this.compiledNodes);
    }

    @Override
    public <T> T getSample(String name, Class<T> type) throws RunException
    {
        Object sample = this.recipients.get(name).getContent();

        if (!type.isAssignableFrom(sample.getClass()))
            throw new RunException("A sample was found for name "+name+" but it was of a different type "+sample.getClass().getName()+" (requested type was "+type.getName()+")");

        //noinspection unchecked
        return (T) sample;
    }

    /**
     *
     * @param compiledNodes
     * @return
     * @throws PipelineRunException
     */
    protected Heap run(List<Node> compiledNodes) throws PipelineRunException
    {
        LinkedList<Node> runQueue = new LinkedList<>(compiledNodes);
        Set<Node> blocked = new HashSet<>();
        Heap heap = new HashHeap();
        StreamHeap streamHeap = new StreamHashHeap(heap);

        /*
         * Fires the whole running queue and discards dead branches resulting from failed executions.
         * Upon a successful run, the outbounds that haven't been added yet are pushed to the waiting queue.
         */
        while (!runQueue.isEmpty())
        {
            Node n = runQueue.poll();
            Heap dynHeap = heap;

            if (blocked.contains(n))
                continue;

            /* If the node is a StreamPipelineNode we need to register a stream round */
            if (n instanceof StreamPipelineNode)
                registerStream((StreamPipelineNode) n, runQueue, heap, streamHeap);
            /* Otherwise we handle it as a standard node */
            else {
                /* If the node is part of a stream round or is an accumulator, we use the stream heap instead of the standard heap */
                if (streamHeap.hasOffset(n) || n instanceof StreamAccumulator)
                    dynHeap = streamHeap;

                if (!this.execution.launch(n, dynHeap))
                    blockBranch(n, blocked);
            }
        }

        return heap;
    }

    /**
     *
     * @param n
     * @param blocked
     */
    private static void blockBranch(Node n, Set<Node> blocked)
    {
        Queue<Node> q = new LinkedList<>();
        q.add(n);
        while (!q.isEmpty())
        {
            Node blockedNode = q.poll();
            blocked.add(blockedNode);
            q.addAll(blockedNode.getDownstream());
        }
    }

    /**
     *
     * @param node
     * @param runQueue
     * @param heap
     * @param streamHeap
     * @throws PipelineRunException
     */
    private static void registerStream(StreamPipelineNode node, LinkedList<Node> runQueue, Heap heap, StreamHeap streamHeap) throws PipelineRunException
    {
        try {
            Generator generator = streamHeap.getGenerator(node, heap);

            if (generator.hasNext())
            {
                Node generatorNode = node.getGeneratorNode();
                int offset = streamHeap.incrementOffset(generatorNode);

                Logging.logger.debug("Launching flow stream generator #"+generatorNode.getUid()+" at offset "+offset+" with generator "+generator.getClass().getName());
                streamHeap.push(generatorNode.getUid(), generator.generate(), generatorNode.getDownstream().size());

                /* If the generator can still produce inputs, we register the pipeline node for another round */
                /* TODO: This could be done without checking, since there is already a generator check  */
                if (generator.hasNext())
                    runQueue.push(node);

                /* Add stream nodes to the top of the queue, note that we need to do in reverse order as we push to the top */
                var reverseIterator = node.getNodes().listIterator(node.getNodes().size());
                while (reverseIterator.hasPrevious())
                {
                    Node streamNode = reverseIterator.previous();
                    runQueue.push(streamNode);
                }
                /* Register current offset for each registered node */
                streamHeap.registerOffset(node.getNodes(), offset);
            }
        }
        catch (GenerationException e) {
            throw new PipelineRunException("An error occurred while attempting to generate a stream input from node.", e, heap);
        }

        //  get generator for stream from stream heap
        //  if generator hasNext
        //    get stream offset from stream heap
        //    register output from generator in stream Heap at offset
        //
        //    if generator hasNext
        //      add node to top of runQueue
        //    add stream nodes to top of runQueue
        //    register offset for each stream node uid
    }

    /**
     *
     * @param compiledNodes
     */
    protected Map<String, Recipient> identifyRecipientNodes(List<Node> compiledNodes)
    {
        Map<String, Recipient> recipients = new HashMap<>();

        for (Node node : compiledNodes)
        {
            if (node instanceof Recipient)
            {
                var recipient = (Recipient) node;
                recipients.put(recipient.getName(), recipient);
            }
        }

        return recipients;
    }
}
