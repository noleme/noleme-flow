package com.noleme.flow.impl.parallel;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.compiler.FlowRuntime;
import com.noleme.flow.compiler.RunException;
import com.noleme.flow.impl.parallel.compiler.ParallelIndexes;
import com.noleme.flow.impl.parallel.runtime.executor.ExecutorServiceProvider;
import com.noleme.flow.impl.parallel.runtime.heap.ConcurrentHashHeap;
import com.noleme.flow.impl.parallel.runtime.state.RuntimeState;
import com.noleme.flow.impl.pipeline.runtime.execution.Execution;
import com.noleme.flow.impl.pipeline.runtime.heap.Heap;
import com.noleme.flow.impl.pipeline.runtime.node.OffsetNode;
import com.noleme.flow.io.input.Input;
import com.noleme.flow.io.output.Output;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamAccumulator;
import com.noleme.flow.stream.StreamGenerator;
import com.noleme.flow.stream.StreamNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
@SuppressWarnings("rawtypes")
public class ParallelRuntime implements FlowRuntime
{
    private final Execution execution;
    private final List<Node> startNodes;
    private final ParallelIndexes indexes;
    private final ExecutorServiceProvider poolProvider;
    private final boolean autoRefresh;
    private ExecutorService pool;
    private CompletionService<Node> completionService;

    private static final Logger logger = LoggerFactory.getLogger(ParallelRuntime.class);
    
    /**
     *
     * @param compiledNodes
     * @param executorServiceProvider
     * @param autoRefresh
     * @param indexes
     */
    protected ParallelRuntime(List<Node> compiledNodes, ExecutorServiceProvider executorServiceProvider, boolean autoRefresh, ParallelIndexes indexes)
    {
        this.execution = new Execution();
        this.startNodes = compiledNodes;
        this.indexes = indexes;
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
    synchronized public Output run(Input input) throws RunException
    {
        Heap heap = new ConcurrentHashHeap(input);
        RuntimeState state = new RuntimeState(this.indexes);

        if (this.pool == null)
            this.regenerateThreadPool();

        /* Add all start nodes to the waiting queue. */
        state.queueAll(this.startNodes);

        try {
            /* We loop as long as we have nodes to process (submitted to the pool or awaiting submission) */
            while (state.hasSubmitted() || state.hasWaiting())
            {
                /* First, we go over the whole waiting list */
                Iterator<Node> waitingIterator = state.waitingIterator();
                while (waitingIterator.hasNext())
                {
                    Node waitingNode = waitingIterator.next();
                    boolean removeIfSubmitted = true;

                    /* If this is a stream generator, its prolonged presence in the waiting list is conditioned by special clauses */
                    if (waitingNode instanceof StreamGenerator)
                    {
                        StreamGenerator generatorNode = (StreamGenerator) waitingNode;
                        Generator generator = heap.getStreamGenerator(generatorNode);

                        /* If the generator is exhausted, we remove it from the waiting pool and skip it */
                        if (!generator.hasNext())
                        {
                            waitingIterator.remove();
                            continue;
                        }
                        /* Generators can't be ran concurrently (as they are expected to be stateful and generated once per run) */
                        if (state.isSubmitted(waitingNode))
                            continue;
                        /* If the stream has reached the max parallelism factor defined on the generator, we keep the node on the waiting pool */
                        if (state.hasReachedMaxParallelism(generatorNode))
                            continue;

                        /* If the generator is to be executed, we do not want to remove it from the waiting pool */
                        removeIfSubmitted = false;
                    }

                    NodeState readiness = this.isReady(waitingNode, state, heap);

                    if (readiness == NodeState.READY)
                    {
                        if (removeIfSubmitted)
                            waitingIterator.remove();

                        this.submitNode(waitingNode, heap, state);
                    }
                    else if (readiness == NodeState.BLOCKED)
                        waitingIterator.remove();
                }
                /* If we have submitted nodes, we use the blocking completion service in order to wait for the first completed node */
                if (state.hasSubmitted())
                {
                    Future<Node> future = this.completionService.take();
                    Node completedNode = future.get();

                    /* We update the state collections */
                    state.unsubmit(completedNode);
                    state.complete(completedNode);

                    /* For each node downstream from the one that just completed, we push it to the waiting list, if it wasn't already */
                    for (Node downstream : completedNode.getDownstream())
                    {
                        /* Non-encapsulated StreamNodes should be ignored (they should already be added as OffsetNodes by the stream trunk */
                        if (downstream instanceof StreamNode)
                            continue;
                        if (state.isWaiting(downstream) || state.isSubmitted(downstream))
                            continue;
                        /*
                         * If an accumulator added by a previous stream node managed to complete in the timespan it takes for the last stream node to return and be considered here, it can create issues.
                         * So we check for completed accumulator nodes too. Note that this should apply to any non-stream node that is designed to be directly downstream of stream nodes.
                         */
                        if (downstream instanceof StreamAccumulator && state.isCompleted(downstream))
                            continue;

                        state.queue(downstream);
                    }
                }
            }

            return heap.getOutput();
        }
        catch (InterruptedException e) {
            throw new ParallelRunException(e.getMessage(), e, heap);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof ParallelRunException)
                throw (ParallelRunException) e.getCause();
            throw new ParallelRunException(e.getMessage(), e, heap);
        }
        finally {
            if (this.autoRefresh)
                this.shutdownThreadPool();
        }
    }

    /**
     *
     * @param node
     * @param heap
     * @param state
     */
    private void submitNode(Node node, Heap heap, RuntimeState state)
    {
        if (node instanceof OffsetNode)
        {
            Node actualNode = ((OffsetNode) node).getNode();
            logger.debug("Submitting flow node #{} offset {} ({})", actualNode.getUid(), ((OffsetNode) node).getOffset(), actualNode.getClass().getSimpleName());
        }
        else
            logger.debug("Submitting flow node #{} ({})", node.getUid(), node.getClass().getSimpleName());

        /*
         * Note regarding behaviour, there are two possible paths here:
         *
         * First is for StreamGenerators, these are responsible for initiating a stream and submitting the first OffsetNode of the stream.
         *
         * Second is for standard nodes and OffsetNodes. OffsetNodes override the getDownstream method so that any downstream StreamNode is returned as an OffsetNode with the same offset value.
         * StreamAccumulators are not StreamNodes, so they will be returned unchanged, and the first stream to finish will submit it to the waiting queue.
         *
         * TODO: it would certainly be a better design to have all state updates in the main loop and get rid of the current RRWLock mess ;
         *       for later: consider returning tuples of (node, result) and perform all state operations after the CompletionService returns
         */
        if (node instanceof StreamGenerator)
        {
            long offset = heap.getNextStreamOffset((StreamGenerator<?, ?>) node);
            OffsetNode offsetNode = new OffsetNode(node, offset);

            state.submit(offsetNode);
            state.initiateStream(offsetNode);

            this.completionService.submit(() -> {
                boolean isSuccess = this.execution.launch(offsetNode, heap);

                if (!isSuccess)
                    state.blockAll(offsetNode.getDownstream());

                state.completeStreamItem(offsetNode);

                return offsetNode;
            });
        }
        else {
            state.submit(node);

            this.completionService.submit(() -> {
                boolean isSuccess = this.execution.launch(node, heap);

                if (!isSuccess)
                    state.blockAll(node.getDownstream());

                if (node instanceof OffsetNode)
                {
                    if (isSuccess)
                        state.completeStreamItem((OffsetNode) node);
                    else
                        state.terminateStream((OffsetNode) node);
                }

                return node;
            });
        }
    }

    /**
     *
     * @param node Target node
     * @param state
     * @param heap
     * @return The current NodeState for the Node
     */
    private NodeState isReady(Node node, RuntimeState state, Heap heap)
    {
        if (node instanceof OffsetNode)
        {
            OffsetNode offsetNode = (OffsetNode) node;
            Node actualNode = offsetNode.getNode();

            if (state.isBlocked(offsetNode) || state.isBlocked(actualNode))
                return NodeState.BLOCKED;
            for (Node usn : actualNode.getUpstream())
            {
                if (!heap.has(usn.getUid(), offsetNode.getOffset()))
                    return NodeState.NOT_READY;
            }
            return NodeState.READY;
        }
        else if (node instanceof StreamAccumulator)
        {
            if (state.isBlocked(node))
                return NodeState.BLOCKED;

            if (!state.isStreamComplete((StreamAccumulator) node))
                return NodeState.NOT_READY;

            for (Node nr : node.getRequirements())
            {
                /* We already take care of stream requirements by checking for the stream completion */
                if (nr instanceof StreamGenerator || nr instanceof StreamNode)
                    continue;

                if (!state.isCompleted(nr))
                    return NodeState.NOT_READY;
            }

            return NodeState.READY;
        }
        else {
            if (state.isBlocked(node))
                return NodeState.BLOCKED;

            for (Node nr : node.getRequirements())
            {
                if (!state.isCompleted(nr))
                    return NodeState.NOT_READY;
            }

            return NodeState.READY;
        }
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
}
