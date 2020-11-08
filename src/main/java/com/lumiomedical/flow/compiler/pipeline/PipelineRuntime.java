package com.lumiomedical.flow.compiler.pipeline;

import com.lumiomedical.flow.compiler.FlowRuntime;
import com.lumiomedical.flow.compiler.RunException;
import com.lumiomedical.flow.compiler.pipeline.execution.Execution;
import com.lumiomedical.flow.compiler.pipeline.heap.HashHeap;
import com.lumiomedical.flow.compiler.pipeline.heap.Heap;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.recipient.Recipient;

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
        Queue<Node> runQueue = new LinkedList<>(compiledNodes);
        Set<Node> blocked = new HashSet<>();
        Heap heap = new HashHeap();

        /*
         * Fires the whole running queue and discards dead branches resulting from failed executions.
         * Upon a successful run, the outbounds that haven't been added yet are pushed to the waiting queue.
         */
        while (!runQueue.isEmpty())
        {
            Node n = runQueue.poll();

            if (blocked.contains(n))
                continue;

            if (!this.execution.launch(n, heap))
                blockBranch(n, blocked);
        }

        return heap;
    }

    /**
     *
     * @param n
     * @param blocked
     */
    protected static void blockBranch(Node n, Set<Node> blocked)
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
