package com.lumiomedical.flow.impl.parallel.compiler;

import com.lumiomedical.flow.node.Node;
import com.lumiomedical.flow.recipient.Recipient;
import com.lumiomedical.flow.stream.StreamGenerator;

import java.util.Map;
import java.util.Set;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/14
 */
public final class ParallelIndexes
{
    public final Map<String, Recipient> recipients;
    public final Map<Node, StreamGenerator> generators;
    public final Map<StreamGenerator, Set<Node>> streamNodes;

    public ParallelIndexes(
        Map<String, Recipient> recipientsIndex,
        Map<Node, StreamGenerator> generatorsIndex,
        Map<StreamGenerator, Set<Node>> streamNodesIndex
    )
    {
        this.recipients = recipientsIndex;
        this.generators = generatorsIndex;
        this.streamNodes = streamNodesIndex;
    }
}
