package com.lumiomedical.flow.compiler.pipeline.stream;

import com.lumiomedical.flow.compiler.NotImplementedException;
import com.lumiomedical.flow.node.AbstractNode;
import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.stream.StreamGenerator;

import java.util.*;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public class StreamPipelineNode extends AbstractNode
{
    private final StreamGenerator generatorNode;
    private final LinkedList<Node> nodes;
    private String pivot;
    private Set<String> potentialPivots;

    /**
     *
     * @param generatorNode
     */
    public StreamPipelineNode(StreamGenerator generatorNode)
    {
        super();
        this.generatorNode = generatorNode;
        this.nodes = new LinkedList<>();
    }

    @Override
    public List<Node> getUpstream()
    {
        return Collections.emptyList();
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

    public StreamPipelineNode push(Node node)
    {
        this.nodes.push(node);
        return this;
    }

    public List<Node> getNodes()
    {
        return this.nodes;
    }

    public StreamPipelineNode setPotentialPivots(Set<String> pivots)
    {
        this.potentialPivots = pivots;
        return this;
    }

    public Set<String> getPotentialPivots()
    {
        return this.potentialPivots;
    }

    public StreamPipelineNode setPivot(String pivot)
    {
        this.pivot = pivot;
        return this;
    }

    public String getPivot()
    {
        return this.pivot;
    }
}
