package com.lumiomedical.flow.node;

import java.util.Collections;
import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public abstract class SimpleNode<T> extends AbstractNode
{
    private final T actor;
    Node upstream;

    /**
     *
     * @param actor
     */
    public SimpleNode(T actor)
    {
        super();
        this.actor = actor;
    }

    /**
     *
     * @return
     */
    public T getActor()
    {
        return this.actor;
    }

    /**
     *
     * @param other
     */
    protected void bind(SimpleNode other)
    {
        if (other.upstream != null)
            throw new RuntimeException(
                "You are attempting an illegal binding: the other node already has an upstream binding towards "
                    + other.upstream.getClass() + "#" + other.upstream.getUid()
            );
        this.downstream.add(other);
        other.upstream = this;
        other.after(this);
    }

    /**
     *
     * @return
     */
    public Node getSimpleUpstream()
    {
        return this.upstream;
    }

    @Override
    public List<Node> getUpstream()
    {
        if (this.upstream == null)
            return Collections.emptyList();
        return Collections.singletonList(this.upstream);
    }
}
