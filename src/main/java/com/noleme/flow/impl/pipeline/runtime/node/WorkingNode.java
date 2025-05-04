package com.noleme.flow.impl.pipeline.runtime.node;

import com.noleme.flow.node.Node;
import com.noleme.flow.node.NodeDecorator;
import com.noleme.flow.stream.StreamGenerator;
import com.noleme.flow.stream.StreamNode;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class WorkingNode<N extends Node> extends NodeDecorator
{
    private final WorkingKey key;
    /* Below acts as caching fields */
    private List<WorkingNode<?>> upstream;
    private List<WorkingNode<?>> downstream;
    private List<WorkingNode<?>> requirements;

    public WorkingNode(N node)
    {
        this(node, null);
    }

    public WorkingNode(N node, WorkingKey parent)
    {
        this(node, -1, parent);
    }

    public WorkingNode(N node, long offset, WorkingKey parent)
    {
        super(node);
        this.key = WorkingKey.of(node, offset, parent);
    }

    public WorkingKey getKey()
    {
        return this.key;
    }

    @SuppressWarnings("unchecked")
    public N getNode()
    {
        return (N) super.getNode();
    }

    @Override
    public List<Node> getUpstream()
    {
        return generalize(this.getWorkingUpstream());
    }

    public List<WorkingNode<?>> getWorkingUpstream()
    {
        if (this.upstream != null)
            return this.upstream;

        synchronized (this)
        {
            if (this.upstream != null)
                return this.upstream;

            this.upstream = this.getNode().getUpstream().stream()
                .map(n -> {
                    if (this.getNode() instanceof StreamGenerator)
                    {
                        return new WorkingNode<>(
                            n,
                            this.key.hasParent() ? this.key.parent().offset() : -1,
                            this.key.hasParent() ? this.key.parent().parent() : null
                        );
                    }
                    else if (isKeyTransitive(n))
                        return new WorkingNode<>(n, this.key.offset(), this.key.parent());
                    return new WorkingNode<>(n, this.key.parent());
                })
                .collect(Collectors.toUnmodifiableList())
            ;
        }
        return this.upstream;
    }

    @Override
    public List<Node> getDownstream()
    {
        return generalize(this.getWorkingDownstream());
    }

    public List<WorkingNode<?>> getWorkingDownstream()
    {
        if (this.downstream != null)
            return this.downstream;

        synchronized (this)
        {
            if (this.downstream != null)
                return this.downstream;

            this.downstream = this.getNode().getDownstream().stream()
                .map(n -> {
                    if (isKeyTransitive(n))
                        return new WorkingNode<>(n, this.key.offset(), this.key.parent());
                    return new WorkingNode<>(n, this.key.parent());
                })
                .collect(Collectors.toUnmodifiableList())
            ;
        }
        return this.downstream;
    }

    @Override
    public List<Node> getRequirements()
    {
        return generalize(this.getWorkingRequirements());
    }

    public List<WorkingNode<?>> getWorkingRequirements()
    {
        if (this.requirements != null)
            return this.requirements;

        synchronized (this)
        {
            if (this.requirements != null)
                return this.requirements;

            this.requirements = this.getNode().getRequirements().stream()
                .map(n -> {
                    if (this.getNode() instanceof StreamGenerator)
                    {
                        return new WorkingNode<>(
                            n,
                            this.key.hasParent() ? this.key.parent().offset() : -1,
                            //this.key.parent().parent()
                            this.key.parent()
                        );
                    }
                    else if (isKeyTransitive(n))
                        return new WorkingNode<>(n, this.key.offset(), this.key.parent());
                    return new WorkingNode<>(n, this.key.parent());
                })
                .collect(Collectors.toUnmodifiableList())
            ;
        }
        return this.requirements;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        WorkingNode<?> that = (WorkingNode<?>) o;
        return this.key.equals(that.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.key);
    }

    /**
     * This is dirty, but we know what we're doing as we're dealing with cached data in unmodifiable lists that we produce ourselves in this very class.
     */
    @SuppressWarnings("unchecked")
    private static List<Node> generalize(List<WorkingNode<?>> nodes)
    {
        return (List<Node>) (List<?>) nodes;
    }

    /**
     * Determines whether a node's key properties (offset and parent) should be transitive to the provided node.
     *
     * @param node
     * @return
     */
    private static boolean isKeyTransitive(Node node)
    {
        return node instanceof StreamNode || node instanceof StreamGenerator;
    }

    /**
     *
     * @param node
     * @return
     */
    public static WorkingNode<?> upstreamOf(WorkingNode<?> node)
    {
        return upstreamOf(node, 0);
    }

    public static WorkingNode<?> upstreamOf(WorkingNode<?> node, int index)
    {
        return node.getWorkingUpstream().get(index);
    }
}
