package com.noleme.flow.impl.pipeline.compiler.stream;

import com.noleme.flow.compiler.NotImplementedException;
import com.noleme.flow.node.AbstractNode;
import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamGenerator;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public class StreamPipeline extends AbstractNode
{
    private final StreamGenerator<?, ?> generatorNode;
    private final List<Node> nodes;
    private final StreamPipeline parent;
    private StreamPipeline topParent;

    /**
     *
     * @param generatorNode
     */
    public StreamPipeline(StreamGenerator<?, ?> generatorNode, StreamPipeline parent)
    {
        super();
        this.generatorNode = generatorNode;
        this.nodes = new ArrayList<>();
        this.setDepth(generatorNode.getDepth());
        this.parent = parent;
        this.topParent = parent != null ? parent.getTopParent() : null;
    }

    public StreamPipeline(StreamGenerator<?, ?> generatorNode)
    {
        this(generatorNode, null);
        this.topParent = this;
    }

    @Override
    public List<Node> getUpstream()
    {
        return this.generatorNode.getUpstream();
    }

    @Override
    public List<Node> getRequirements()
    {
        return this.generatorNode.getRequirements();
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

    public StreamGenerator getGeneratorNode()
    {
        return this.generatorNode;
    }

    public StreamPipeline add(Node node)
    {
        this.nodes.add(node);
        return this;
    }

    public List<Node> getNodes()
    {
        return this.nodes;
    }

    public StreamPipeline getParent()
    {
        return this.parent;
    }

    public StreamPipeline getTopParent()
    {
        return this.topParent;
    }
}
