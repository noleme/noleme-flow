package com.lumiomedical.flow.impl.parallel.runtime.state;

import com.lumiomedical.flow.impl.parallel.compiler.ParallelIndexes;
import com.lumiomedical.flow.impl.pipeline.runtime.node.OffsetNode;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.stream.StreamAccumulator;
import com.lumiomedical.flow.stream.StreamGenerator;
import com.lumiomedical.flow.stream.StreamNode;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/14
 */
public class RuntimeState
{
    private final Map<StreamGenerator, ParallelismCounter> streamParallelism = new ConcurrentHashMap<>();
    private final Map<OffsetNode, Set<OffsetNode>> streamChecklist = new ConcurrentHashMap<>();
    private final Set<Node> waiting = new ConcurrentSkipListSet<>();
    private final Set<Node> submitted = new ConcurrentSkipListSet<>();
    private final Set<Node> completed = new ConcurrentSkipListSet<>();
    private final Set<Node> blocked = new ConcurrentSkipListSet<>();
    private final Set<StreamGenerator> submittedGenerators = new ConcurrentSkipListSet<>();
    private final ParallelIndexes indexes;

    public RuntimeState(ParallelIndexes indexes)
    {
        this.indexes = indexes;
    }

    public RuntimeState queue(Node node)
    {
        this.waiting.add(node);
        return this;
    }

    public RuntimeState queueAll(Collection<Node> nodes)
    {
        this.waiting.addAll(nodes);
        return this;
    }

    public RuntimeState unqueue(Node node)
    {
        this.waiting.remove(node);
        return this;
    }

    public RuntimeState submit(Node node)
    {
        if (node instanceof OffsetNode && ((OffsetNode) node).getNode() instanceof StreamGenerator)
            this.submittedGenerators.add((StreamGenerator) ((OffsetNode) node).getNode());

        this.submitted.add(node);
        return this;
    }

    public RuntimeState submitAll(Collection<Node> nodes)
    {
        this.submitted.addAll(nodes);
        return this;
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    public RuntimeState unsubmit(Node node)
    {
        if (node instanceof OffsetNode && ((OffsetNode) node).getNode() instanceof StreamGenerator)
            this.submittedGenerators.remove(((OffsetNode) node).getNode());

        this.submitted.remove(node);
        return this;
    }

    public RuntimeState complete(Node node)
    {
        this.completed.add(node);
        return this;
    }

    public RuntimeState completeAll(Collection<Node> nodes)
    {
        this.completed.addAll(nodes);
        return this;
    }

    public RuntimeState uncomplete(Node node)
    {
        this.completed.remove(node);
        return this;
    }

    public RuntimeState block(Node node)
    {
        this.blocked.add(node);
        return this;
    }

    public RuntimeState blockAll(Collection<Node> nodes)
    {
        this.blocked.addAll(nodes);
        return this;
    }

    public RuntimeState unblock(Node node)
    {
        this.blocked.remove(node);
        return this;
    }

    public Iterator<Node> waitingIterator()
    {
        return this.waiting.iterator();
    }

    public boolean isWaiting(Node node)
    {
        return this.waiting.contains(node);
    }

    public boolean isSubmitted(Node node)
    {
        if (node instanceof StreamGenerator)
            return this.submittedGenerators.contains(node);

        return this.submitted.contains(node);
    }

    public boolean isCompleted(Node node)
    {
        if (node instanceof StreamNode)
            return this.isStreamComplete((StreamNode) node);

        return this.completed.contains(node);
    }

    public boolean isBlocked(Node node)
    {
        return this.blocked.contains(node);
    }

    public boolean hasWaiting()
    {
        return !this.waiting.isEmpty();
    }

    public boolean hasSubmitted()
    {
        return !this.submitted.isEmpty();
    }

    /**
     *
     * @param node
     * @return
     */
    private StreamGenerator getGenerator(Node node)
    {
        return node instanceof StreamGenerator
            ? (StreamGenerator) node
            : this.indexes.generators.get(node)
        ;
    }

    /**
     *
     * @param offsetGenerator
     */
    synchronized public void initiateStream(OffsetNode offsetGenerator)
    {
        StreamGenerator generator = (StreamGenerator) offsetGenerator.getNode();

        this.streamChecklist.put(offsetGenerator, this.indexes.streamNodes.get(generator).stream()
            .map(sn -> new OffsetNode(sn, offsetGenerator.getOffset()))
            .collect(Collectors.toCollection(ConcurrentSkipListSet::new))
        );
        this.getParallelism(generator).increment();
    }

    /**
     *
     * @param node
     */
    synchronized public void completeStreamItem(OffsetNode node)
    {
        StreamGenerator generator = this.getGenerator(node.getNode());
        Set<OffsetNode> checklist = this.streamChecklist.get(new OffsetNode(generator, node.getOffset()));
        checklist.remove(node);

        if (checklist.isEmpty())
            this.getParallelism(generator).decrement();
    }

    /**
     *
     * @param accumulator
     * @return
     */
    public boolean isStreamComplete(StreamAccumulator accumulator)
    {
        StreamGenerator generator = this.getGenerator(accumulator.getSimpleUpstream());

        if (this.isWaiting(generator))
            return false;

        return this.getParallelism(generator).counter == 0;
    }

    /**
     *
     * @param node
     * @return
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public boolean isStreamComplete(StreamNode node)
    {
        StreamGenerator generator = this.indexes.generators.get(node);

        /* If the stream was never started, it cannot be completed */
        if (!this.streamParallelism.containsKey(generator))
            return false;
        /* If the generator is still in waiting, it cannot be completed */
        if (this.isWaiting(generator))
            return false;

        /* If the generator is not in waiting and parallelism count is set to 0, all downstream stream nodes should be completed */
        return this.getParallelism(generator).counter == 0;
    }

    /**
     *
     * @param node
     * @return
     */
    public boolean hasReachedMaxParallelism(StreamGenerator node)
    {
        return this.streamParallelism.containsKey(node)
            && this.streamParallelism.get(node).count() >= node.getMaxParallelism()
        ;
    }

    /**
     *
     * @param node
     * @return
     */
    synchronized public ParallelismCounter getParallelism(StreamGenerator node)
    {
        if (!this.streamParallelism.containsKey(node))
            this.streamParallelism.put(node, new ParallelismCounter());
        return this.streamParallelism.get(node);
    }

    /**
     *
     * @param waitingNode
     * @return
     */
    public boolean isGeneratorSubmitted(StreamGenerator waitingNode)
    {
        return false;
    }

    public static class ParallelismCounter
    {
        private int counter = 0;

        public int count() { return this.counter; }
        public int increment() { return ++this.counter; }
        public int decrement() { return --this.counter; }
    }
}
