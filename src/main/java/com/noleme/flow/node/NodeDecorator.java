package com.noleme.flow.node;

import com.noleme.flow.compiler.NotImplementedException;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/12
 */
public abstract class NodeDecorator implements Node
{
    private final Node node;

    public NodeDecorator(Node node)
    {
        this.node = node;
    }

    /**
     *
     * @return
     */
    public Node getNode()
    {
        return this.node;
    }

    @Override
    public String getUid()
    {
        return this.node.getUid();
    }

    @Override
    public List<Node> getDownstream()
    {
        return this.node.getDownstream();
    }

    @Override
    public List<Node> getUpstream()
    {
        return this.node.getUpstream();
    }

    @Override
    public List<Node> getRequiredBy()
    {
        return this.node.getRequiredBy();
    }

    @Override
    public List<Node> getRequirements()
    {
        return this.node.getRequirements();
    }

    @Override
    public Node after(Node other)
    {
        throw new NotImplementedException();
    }

    @Override
    public Node after(Collection<Node> others)
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Node))
            return false;
        Node that = (Node) o;
        return this.getUid().equals(that.getUid());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.getUid());
    }
}
