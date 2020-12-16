package com.lumiomedical.flow.impl.parallel;

import com.lumiomedical.flow.compiler.RunException;
import com.lumiomedical.flow.impl.pipeline.runtime.heap.Heap;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public class ParallelRunException extends RunException
{
    private final Heap heap;

    public ParallelRunException(String message, Heap heap)
    {
        super(message);
        this.heap = heap;
    }

    public ParallelRunException(String message, Throwable cause, Heap heap)
    {
        super(message, cause);
        this.heap = heap;
    }

    public Heap getHeap()
    {
        return heap;
    }
}
