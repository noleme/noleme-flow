package com.lumiomedical.flow.impl.parallel.compiler;

import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.stream.StreamGenerator;

import java.util.Map;
import java.util.Set;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/14
 */
public final class ParallelIndexes
{
    public final Map<Node, StreamGenerator> generators;
    public final Map<StreamGenerator, Set<Node>> streamNodes;

    public ParallelIndexes(
        Map<Node, StreamGenerator> generatorsIndex,
        Map<StreamGenerator, Set<Node>> streamNodesIndex
    )
    {
        this.generators = generatorsIndex;
        this.streamNodes = streamNodesIndex;
    }
}
