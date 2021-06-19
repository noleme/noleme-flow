package com.noleme.flow.impl.parallel.runtime.state;

import com.noleme.flow.impl.parallel.compiler.ParallelIndexes;
import com.noleme.flow.impl.pipeline.PipelineRuntime;
import com.noleme.flow.impl.pipeline.runtime.node.OffsetNode;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamAccumulator;
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
    private final Map<OffsetNode, Set<OffsetNode>> streamChecklist = new HashMap<>();
    private final Set<Node> waiting = new HashSet<>();
    private final Set<Node> submitted = new HashSet<>();
    private final Set<Node> completed = new HashSet<>();
    private final Set<Node> blocked = new HashSet<>();
    private final Set<StreamGenerator> submittedGenerators = new HashSet<>();
    private final ParallelIndexes indexes;
    private final RRWLock blockLock = new RRWLock();
    private final RRWLock streamLock = new RRWLock();

    public RuntimeState(ParallelIndexes indexes)
    {
        this.indexes = indexes.copy();
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

    public RuntimeState submit(Node node)
    {
        if (node instanceof OffsetNode && ((OffsetNode) node).getNode() instanceof StreamGenerator)
            this.submittedGenerators.add((StreamGenerator) ((OffsetNode) node).getNode());

        this.submitted.add(node);
        return this;
    }

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

    public RuntimeState block(Node node)
    {
        try {
            this.blockLock.write.lock();
            this.blocked.add(node);
            return this;
        }
        finally {
            this.blockLock.write.unlock();
        }
    }

    public RuntimeState blockAll(Collection<Node> nodes)
    {
        try {
            this.blockLock.write.lock();
            for (Node node : nodes)
                PipelineRuntime.blockBranch(node, this.blocked);
            return this;
        }
        finally {
            this.blockLock.write.unlock();
        }
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
    private StreamGenerator<?, ?> getGenerator(Node node)
    {
        return node instanceof StreamGenerator
            ? (StreamGenerator<?, ?>) node
            : this.indexes.generators.get(node)
        ;
    }

    /**
     *
     * @param offsetGenerator
     */
    public void initiateStream(OffsetNode offsetGenerator)
    {
        try {
            this.streamLock.write.lock();
            StreamGenerator generator = (StreamGenerator) offsetGenerator.getNode();

            this.streamChecklist.put(offsetGenerator, this.indexes.streamNodes.get(generator).stream()
                .map(sn -> new OffsetNode(sn, offsetGenerator.getOffset()))
                .collect(Collectors.toCollection(ConcurrentSkipListSet::new))
            );

            this.parallelism.increase(generator);
        }
        finally {
            this.streamLock.write.unlock();
        }
    }

    /**
     *
     * @param node
     */
    public void completeStreamItem(OffsetNode node)
    {
        try {
            this.streamLock.write.lock();
            StreamGenerator generator = this.getGenerator(node.getNode());
            Set<OffsetNode> checklist = this.streamChecklist.get(new OffsetNode(generator, node.getOffset()));
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
    public void terminateStream(OffsetNode node)
    {
        try {
            this.streamLock.write.lock();
            StreamGenerator generator = this.getGenerator(node.getNode());
            OffsetNode key = new OffsetNode(generator, node.getOffset());
            if (!this.streamChecklist.containsKey(key))
                return;
            this.streamChecklist.put(key, new HashSet<>());
            this.parallelism.decrease(generator);
        }
        finally {
            this.streamLock.write.unlock();
        }
    }

    /**
     *
     * @param accumulator
     * @return
     */
    public boolean isStreamComplete(StreamAccumulator<?, ?> accumulator)
    {
        try {
            this.streamLock.read.lock();
            StreamGenerator generator = this.getGenerator(accumulator.getSimpleUpstream());

            if (this.isWaiting(generator))
                return false;

            return this.parallelism.isIdle(generator);
        }
        finally {
            this.streamLock.read.unlock();
        }
    }

    /**
     *
     * @param node
     * @return
     */
    public boolean isStreamComplete(StreamNode node)
    {
        try {
            this.streamLock.read.lock();
            StreamGenerator generator = this.indexes.generators.get(node);

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

    public String dump() {
        var sb = new StringBuilder();

        sb.append(this).append("\n");
        sb.append("  blocklock:\n");
        sb.append("    read: ").append(this.blockLock.read).append("\n");
        sb.append("    write: ").append(this.blockLock.write).append("\n");
        sb.append("  streamlock:\n");
        sb.append("    read: ").append(this.streamLock.read).append("\n");
        sb.append("    write: ").append(this.streamLock.write).append("\n");
        sb.append("  waiting:\n");
        this.waiting.forEach(node -> {
            sb.append("    ").append(node.getUid()).append(": ").append(node).append("\n");
        });
        sb.append("  submitted:\n");
        this.submitted.forEach(node -> {
            sb.append("    ").append(node.getUid()).append(": ").append(node).append("\n");
        });
        sb.append("  completed:\n");
        this.completed.forEach(node -> {
            sb.append("    ").append(node.getUid()).append(": ").append(node).append("\n");
        });
        sb.append("  blocked:\n");
        this.blocked.forEach(node -> {
            sb.append("    ").append(node.getUid()).append(": ").append(node).append("\n");
        });
        sb.append("  streams:\n");
        sb.append("    generators:\n");
        this.submittedGenerators.forEach(node -> {
            sb.append("      ").append(node.getUid()).append(": ").append(node).append("\n");
        });
        sb.append("    checklists:\n");
        this.streamChecklist.forEach((offsetNode, checklist) -> {
            sb.append("      ").append(offsetNode.getUid()).append(":");
            if (checklist.isEmpty())
                sb.append(" empty\n");
            else {
                sb.append("\n");
                checklist.forEach(node -> {
                    sb.append("        ").append(node.getUid()).append(": ").append(node.getNode()).append("\n");
                });
            }
        });

        return sb.toString();
    }
}
