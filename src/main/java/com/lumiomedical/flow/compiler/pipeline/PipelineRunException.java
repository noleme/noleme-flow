package com.lumiomedical.flow.compiler.pipeline;

import com.lumiomedical.flow.compiler.RunException;
import com.lumiomedical.flow.compiler.pipeline.heap.Heap;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public class PipelineRunException extends RunException
{
    private final Heap heap;

    public PipelineRunException(String message, Heap heap)
    {
        super(message);
        this.heap = heap;
    }

    public PipelineRunException(String message, Throwable cause, Heap heap)
    {
        super(message, cause);
        this.heap = heap;
    }

    public Heap getHeap()
    {
        return heap;
    }
}
