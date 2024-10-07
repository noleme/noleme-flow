package com.noleme.flow.impl.parallel.runtime.state;

import com.noleme.flow.impl.parallel.compiler.ParallelIndexes;
import com.noleme.flow.impl.pipeline.PipelineRuntime;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingKey;
import com.noleme.flow.impl.pipeline.runtime.node.WorkingNode;
import com.noleme.flow.stream.StreamGenerator;
import com.noleme.flow.stream.StreamNode;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/14
 */
@SuppressWarnings("rawtypes")
public class RuntimeState
{
    private final ParallelismState parallelism = new ParallelismState();
    /* Each stream generation offset has its own checklist (hence why we index by the offset generator, and not the generator itself) */
    private final Map<WorkingNode<StreamGenerator>, Set<WorkingNode<?>>> streamChecklist = new HashMap<>();
    private final Set<WorkingNode<?>> waiting = new HashSet<>();
    private final Set<WorkingNode<?>> submitted = new HashSet<>();
    private final Set<WorkingNode<?>> completed = new HashSet<>();
    private final Set<WorkingKey> blocked = new HashSet<>();
    private final Set<WorkingNode<StreamGenerator>> submittedGenerators = new HashSet<>();
    private final ParallelIndexes indexes;
    private final RRWLock blockLock = new RRWLock();
    private final RRWLock streamLock = new RRWLock();

    public RuntimeState(ParallelIndexes indexes)
    {
        this.indexes = indexes.copy();
    }

    public RuntimeState queue(WorkingNode<?> node)
    {
        this.waiting.add(node);
        return this;
    }

    public RuntimeState queueAll(Collection<WorkingNode<?>> nodes)
    {
        this.waiting.addAll(nodes);
        return this;
    }

    @SuppressWarnings("unchecked")
    public RuntimeState submit(WorkingNode<?> node)
    {
        if (node.getNode() instanceof StreamGenerator)
            this.submittedGenerators.add((WorkingNode<StreamGenerator>) node);

        this.submitted.add(node);
        return this;
    }

    public RuntimeState unsubmit(WorkingNode<?> node)
    {
        if (node.getNode() instanceof StreamGenerator)
            this.submittedGenerators.remove(node);

        this.submitted.remove(node);
        return this;
    }

    public RuntimeState complete(WorkingNode<?> node)
    {
        this.completed.add(node);
        return this;
    }

    public RuntimeState block(WorkingNode<?> node)
    {
        try {
            this.blockLock.write.lock();
            this.blocked.add(node.getKey());
            return this;
        }
        finally {
            this.blockLock.write.unlock();
        }
    }

    public RuntimeState blockAll(Collection<WorkingNode<?>> nodes)
    {
        try {
            this.blockLock.write.lock();
            for (WorkingNode<?> node : nodes)
                PipelineRuntime.blockBranch(node, this.blocked);
            return this;
        }
        finally {
            this.blockLock.write.unlock();
        }
    }

    public Iterator<WorkingNode<?>> waitingIterator()
    {
        return this.waiting.iterator();
    }

    public boolean isWaiting(WorkingNode<?> node)
    {
        return this.waiting.contains(node);
    }

    public boolean isSubmitted(WorkingNode<?> node)
    {
        if (node.getNode() instanceof StreamGenerator)
            return this.submittedGenerators.contains(node);

        return this.submitted.contains(node);
    }

    public boolean isCompleted(WorkingNode<?> node)
    {
        if (node.getNode() instanceof StreamNode)
            return this.isStreamComplete(node);

        return this.completed.contains(node);
    }

    public boolean isBlocked(WorkingNode<?> node)
    {
        try {
            this.blockLock.read.lock();
            return this.blocked.contains(node);
        }
        finally {
            this.blockLock.read.unlock();
        }
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
    private WorkingNode<StreamGenerator> getGenerator(WorkingNode<?> node)
    {
        return node.getNode() instanceof StreamGenerator
            ? (WorkingNode<StreamGenerator>) node
            : new WorkingNode<>(this.indexes.generators.get(node), node.getKey())
        ;
    }

    /**
     *
     * @param generatorNode
     */
    public void initiateStream(WorkingNode<StreamGenerator> generatorNode)
    {
        try {
            this.streamLock.write.lock();
            StreamGenerator generator = generatorNode.getNode();

            this.streamChecklist.put(generatorNode, this.indexes.streamNodes.get(generator).stream()
                .map(sn -> new WorkingNode<>(sn, generatorNode.getKey()))
                .collect(Collectors.toCollection(ConcurrentSkipListSet::new))
            );

            this.parallelism.increase(generatorNode);
        }
        finally {
            this.streamLock.write.unlock();
        }
    }

    /**
     *
     * @param node
     */
    public void completeStreamItem(WorkingNode<?> node)
    {
        try {
            this.streamLock.write.lock();
            WorkingNode<StreamGenerator> generator = this.getGenerator(node);
            Set<WorkingNode<?>> checklist = this.streamChecklist.get(generator);
            checklist.remove(node);

            if (checklist.isEmpty())
                this.parallelism.decrease(generator);
        }
        finally {
            this.streamLock.write.unlock();
        }
    }

    /**
     *
     * @param node
     */
    public void terminateStream(WorkingNode<?> node)
    {
        try {
            this.streamLock.write.lock();
            WorkingNode<StreamGenerator> generator = this.getGenerator(node);
            if (!this.streamChecklist.containsKey(generator))
                return;
            this.streamChecklist.put(generator, new HashSet<>());
            this.parallelism.decrease(generator);
        }
        finally {
            this.streamLock.write.unlock();
        }
    }

    /**
     *
     * @param node
     * @return
     */
    public boolean isStreamComplete(WorkingNode<?> node)
    {
        try {
            this.streamLock.read.lock();
            WorkingNode<StreamGenerator> generator = this.getGenerator(node);

            /* If the stream was never started, it cannot be completed */
            if (!this.parallelism.has(generator))
                return false;
            /* If the generator is still in waiting, it cannot be completed */
            if (this.isWaiting(generator))
                return false;

            /* If the generator is not in waiting and parallelism count is set to 0, all downstream stream nodes should be completed */
            return this.parallelism.isIdle(generator);
        }
        finally {
            this.streamLock.read.unlock();
        }
    }

    /**
     *
     * @param generatorNode
     * @return
     */
    public boolean hasReachedMaxParallelism(StreamGenerator generatorNode)
    {
        return this.parallelism.hasReachedMax(generatorNode);
    }
}
