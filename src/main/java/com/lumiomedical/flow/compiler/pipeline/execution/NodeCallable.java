package com.lumiomedical.flow.compiler.pipeline.execution;

import com.lumiomedical.flow.compiler.pipeline.heap.Heap;
import com.lumiomedical.flow.node.Node;

import java.util.concurrent.Callable;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/10/19
 */
public class NodeCallable implements Callable<Node>
{
    private final Execution execution;
    private final Node node;
    private final Heap heap;

    /**
     *
     * @param execution
     * @param node
     * @param heap
     */
    public NodeCallable(Execution execution, Node node, Heap heap)
    {
        this.execution = execution;
        this.node = node;
        this.heap = heap;
    }

    @Override
    public Node call() throws Exception
    {
        this.execution.launch(this.node, this.heap);

        return this.node;
    }
}
