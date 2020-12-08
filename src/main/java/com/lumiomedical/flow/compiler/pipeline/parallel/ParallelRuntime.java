package com.lumiomedical.flow.compiler.pipeline.parallel;

import com.lumiomedical.flow.compiler.pipeline.PipelineRunException;
import com.lumiomedical.flow.compiler.pipeline.PipelineRuntime;
import com.lumiomedical.flow.compiler.pipeline.execution.NodeCallable;
import com.lumiomedical.flow.compiler.pipeline.heap.ConcurrentHashHeap;
import com.lumiomedical.flow.compiler.pipeline.heap.Heap;
import com.lumiomedical.flow.compiler.pipeline.parallel.executor.ExecutorServiceProvider;
import com.lumiomedical.flow.logger.Logging;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.recipient.Recipient;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
public class ParallelRuntime extends PipelineRuntime
{
    private final ExecutorServiceProvider poolProvider;
    private final boolean autoRefresh;
    private ExecutorService pool;
    private CompletionService<Node> completionService;

    /**
     *
     * @param compiledNodes
     * @param executorServiceProvider
     */
    public ParallelRuntime(List<Node> compiledNodes, ExecutorServiceProvider executorServiceProvider, boolean autoRefresh)
    {
        super(compiledNodes);
        this.poolProvider = executorServiceProvider;
        this.autoRefresh = autoRefresh;
        this.regenerateThreadPool();
    }

    /**
     *
     */
    synchronized private void regenerateThreadPool()
    {
        this.pool = this.poolProvider.provide();
        this.completionService = new ExecutorCompletionService<>(this.pool);
    }

    /**
     *
     */
    synchronized public void shutdownThreadPool()
    {
        if (this.pool != null)
        {
            this.pool.shutdown();
            this.pool = null;
            this.completionService = null;
        }
    }

    @Override
    synchronized protected Heap run(List<Node> startNodes) throws PipelineRunException
    {
        Heap heap = new ConcurrentHashHeap();
        Set<Node> waiting = new ConcurrentSkipListSet<>();
        Set<Node> submitted = new ConcurrentSkipListSet<>();
        Set<Node> completed = new ConcurrentSkipListSet<>();
        Set<Node> blocked = new ConcurrentSkipListSet<>();

        if (this.pool == null)
            this.regenerateThreadPool();

        /* Submits each start node for the pool to process. */
        for (Node node : startNodes)
            this.submitNode(node, heap, submitted);

        try {
            /* We loop as long as we have nodes to process (submitted to the pool or awaiting submission) */
            while (submitted.size() > 0 || waiting.size() > 0)
            {
                /* First, we go over the whole waiting list */
                Iterator<Node> waitingIterator = waiting.iterator();
                while (waitingIterator.hasNext())
                {
                    Node waitingNode = waitingIterator.next();

                    /* If a waiting node is ready to be executed, we submit it */
                    if (this.isReady(waitingNode, blocked, completed, heap) == NodeState.READY)
                    {
                        waitingIterator.remove();
                        this.submitNode(waitingNode, heap, submitted);
                    }
                }

                /* If we have submitted nodes, we use the blocking completion service in order to wait for the first completed node */
                if (submitted.size() > 0)
                {
                    Future<Node> future = this.completionService.take();
                    Node completedNode = future.get();

                    /* We update the state collections */
                    submitted.remove(completedNode);
                    completed.add(completedNode);

                    /* For each node downstream from the one that just completed, we push it to the waiting list, if it wasn't already */
                    for (Node downstream : completedNode.getDownstream())
                    {
                        if (waiting.contains(downstream) || submitted.contains(downstream))
                            continue;

                        waiting.add(downstream);
                    }
                }
            }
        }
        catch (InterruptedException e) {
            throw new PipelineRunException(e.getMessage(), e, heap);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof PipelineRunException)
                throw (PipelineRunException) e.getCause();
            throw new PipelineRunException(e.getMessage(), e, heap);
        }
        finally {
            if (this.autoRefresh)
                this.shutdownThreadPool();
        }

        return heap;
    }

    /**
     *
     * @param node
     * @param heap
     * @param submitted
     */
    private void submitNode(Node node, Heap heap, Set<Node> submitted)
    {
        Logging.logger.debug("Submitting flow node #"+node.getUid()+" ("+node.getClass().getSimpleName()+")");

        submitted.add(node);

        this.completionService.submit(
            new NodeCallable(this.execution, node, heap)
        );
    }

    /**
     *
     * @param node Target node
     * @param blocked
     * @param completed
     * @param heap
     * @return The current NodeState for the Node
     */
    protected NodeState isReady(Node node, Set<Node> blocked, Set<Node> completed, Heap heap)
    {
        if (blocked.contains(node))
            return NodeState.BLOCKED;

        for (Node nr : node.getRequirements())
        {
            if (!completed.contains(nr))
                return NodeState.NOT_READY;
        }

        return NodeState.READY;
    }

    /**
     * The NodeState enum represents the different states a Node can be in at runtime before its execution.
     * READY is for Nodes that are ready to be executed
     * NOT_READY is for Nodes that aren't yet ready to be executed
     * BLOCKED is for Nodes that will not be executed either because they will never get all of their inputs, or because a parent Node blocked their branch
     */
    protected enum NodeState
    {
        READY, NOT_READY, BLOCKED
    }

    @Override
    protected Map<String, Recipient> identifyRecipientNodes(List<Node> compiledNodes)
    {
        Map<String, Recipient> recipients = new HashMap<>();
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

        return recipients;
    }
}
