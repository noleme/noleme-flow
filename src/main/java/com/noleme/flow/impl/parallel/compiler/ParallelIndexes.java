package com.noleme.flow.impl.parallel.compiler;

import com.noleme.flow.node.Node;
import com.noleme.flow.stream.StreamGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/14
 */
@SuppressWarnings("rawtypes")
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

    public ParallelIndexes copy()
    {
        return new ParallelIndexes(
            new HashMap<>(this.generators),
            new HashMap<>(this.streamNodes)
        );
    }
}
