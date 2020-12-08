package com.lumiomedical.flow.compiler.pipeline.stream;

import com.lumiomedical.flow.actor.generator.Generator;
import com.lumiomedical.flow.compiler.pipeline.heap.Heap;
import com.lumiomedical.flow.node.Node;

import java.util.Collection;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/05
 */
public interface StreamHeap extends Heap
{
    /**
     *
     * @param streamNode
     * @param heap
     * @return
     */
    Generator getGenerator(StreamPipelineNode streamNode, Heap heap);

    /**
     *
     * @param node
     * @return
     */
    int incrementOffset(Node node);

    /**
     *
     * @param node
     * @return
     */
    boolean hasOffset(Node node);

    /**
     *
     * @param nodes
     * @param offset
     * @return
     */
    StreamHeap registerOffset(Collection<Node> nodes, int offset);

    /**
     *
     * @param id
     * @return
     */
    Collection<Object> consumeAll(String id);
}
