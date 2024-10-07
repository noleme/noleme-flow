package com.noleme.flow.impl.pipeline.runtime.node;

import com.noleme.flow.node.Node;

import java.util.Objects;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public final class WorkingKey
{
    private final String identifier;
    private final long offset;
    private final WorkingKey parent;

    private WorkingKey(String identifier, long offset, WorkingKey parent)
    {
        this.identifier = identifier;
        this.offset = offset;
        this.parent = parent;
    }

    public static WorkingKey of(Node node)
    {
        return new WorkingKey(node.getUid(), -1, null);
    }

    public static WorkingKey of(Node node, WorkingKey parent)
    {
        return new WorkingKey(node.getUid(), -1, parent);
    }

    public static WorkingKey of(Node node, long offset)
    {
        return new WorkingKey(node.getUid(), offset, null);
    }

    public static WorkingKey of(Node node, long offset, WorkingKey parent)
    {
        return new WorkingKey(node.getUid(), offset, parent);
    }

    public WorkingKey withoutOffset()
    {
        return new WorkingKey(this.identifier, -1, this.parent);
    }

    public String identifier()
    {
        return this.identifier;
    }

    public long offset()
    {
        return this.offset;
    }

    public WorkingKey parent()
    {
        return this.parent;
    }

    public boolean hasOffset()
    {
        return this.offset >= 0;
    }

    public boolean hasParent()
    {
        return this.parent != null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        WorkingKey streamKey = (WorkingKey) o;
        return this.offset == streamKey.offset
            && Objects.equals(this.identifier, streamKey.identifier)
            && Objects.equals(this.parent, streamKey.parent)
        ;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.identifier, this.offset, this.parent);
    }

    public String toString()
    {
        if (!this.hasOffset())
            return this.identifier;
        return this.identifier+"@"+this.offset;
    }
}
