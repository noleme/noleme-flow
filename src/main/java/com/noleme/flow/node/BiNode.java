package com.noleme.flow.node;

import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public abstract class BiNode<T> extends AbstractNode
{
    private final List<Node> upstream;
    private final T actor;

    /**
     *
     * @param upstream1
     * @param upstream2
     */
    public BiNode(Node upstream1, Node upstream2, T actor)
    {
        this.actor = actor;
        this.upstream = List.of(upstream1, upstream2);
        upstream1.getDownstream().add(this);
        upstream2.getDownstream().add(this);
        this.after(upstream1);
        this.after(upstream2);
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
    protected void bind(SimpleNode<?> other)
    {
        if (other.upstream != null)
            throw new RuntimeException("You are attempting an illegal binding: the other node already has an upstream binding towards " + other.upstream.getClass() + "#" + other.upstream.getUid());

        this.downstream.add(other);
        other.upstream = this;
        other.after(this);
    }

    /**
     *
     * @return
     */
    public Node getUpstream1()
    {
        return this.upstream.get(0);
    }

    /**
     *
     * @return
     */
    public Node getUpstream2()
    {
        return this.upstream.get(1);
    }

    @Override
    public List<Node> getUpstream()
    {
        return this.upstream;
    }
}
