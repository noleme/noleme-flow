package com.lumiomedical.flow.node;

import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public abstract class BiNode extends AbstractNode
{
    private final Node upstream1;
    private final Node upstream2;
    /* Generated for optimization purposes */
    private final List<Node> upstreamList;

    /**
     *
     * @param upstream1
     * @param upstream2
     */
    public BiNode(Node upstream1, Node upstream2)
    {
        this.upstream1 = upstream1;
        this.upstream2 = upstream2;
        this.upstream1.getDownstream().add(this);
        this.upstream2.getDownstream().add(this);
        this.upstreamList = List.of(upstream1, upstream2);
        this.after(this.upstream1);
        this.after(this.upstream2);
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
    public Node getUpstream1()
    {
        return upstream1;
    }

    /**
     *
     * @return
     */
    public Node getUpstream2()
    {
        return upstream2;
    }

    @Override
    public List<Node> getUpstream()
    {
        return this.upstreamList;
    }
}
