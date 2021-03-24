package com.noleme.flow.impl.pipeline.runtime.node;

import com.noleme.flow.node.Node;
import com.noleme.flow.node.NodeDecorator;
import com.noleme.flow.stream.StreamNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/12
 */
public final class OffsetNode extends NodeDecorator
{
    private final String uid;
    private final int offset;
    private List<Node> downstream;
    private List<Node> requirements;
    //private List<Node> requiredBy;

    /**
     *
     * @param node
     * @param offset
     */
    public OffsetNode(Node node, int offset)
    {
        super(node);
        this.offset = offset;
        this.uid = node.getUid() + "#"+this.offset;
    }

    /**
     *
     * @return
     */
    public String getUid()
    {
        return this.uid;
    }

    public int getOffset()
    {
        return this.offset;
    }

    @Override
    public List<Node> getRequirements()
    {
        return this.getOffsetRequirements();
    }

    @Override
    public List<Node> getDownstream()
    {
        return this.getOffsetDownstream();
    }

    /**
     *
     * @return
     */
    private List<Node> getOffsetDownstream()
    {
        if (this.downstream != null)
            return this.downstream;

        synchronized (this) {
            if (this.downstream != null)
                return this.downstream;

            this.downstream = this.getNode().getDownstream().stream()
                .map(n -> {
                    if (n instanceof StreamNode)
                        return new OffsetNode(n, this.offset);
                    return n;
                })
                .collect(Collectors.toList())
            ;

        }
        return this.downstream;
    }

    /**
     *
     * @return
     */
    private List<Node> getOffsetRequirements()
    {
        if (this.requirements != null)
            return this.requirements;

        synchronized (this) {
            if (this.requirements != null)
                return this.requirements;

            this.requirements = this.getNode().getRequirements().stream()
                .map(n -> {
                    if (n instanceof StreamNode)
                        return new OffsetNode(n, this.offset);
                    return n;
                })
                .collect(Collectors.toList())
            ;
        }

        return this.requirements;
    }
}
