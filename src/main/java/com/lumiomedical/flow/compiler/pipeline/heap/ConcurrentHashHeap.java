package com.lumiomedical.flow.compiler.pipeline.heap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Pierre Lecerf (pierre.lecerf@gmail.com) on 23/07/15.
 */
public class ConcurrentHashHeap implements Heap
{
    private final Map<String, Counter> contents;

    public ConcurrentHashHeap()
    {
        super();
        this.contents = new ConcurrentHashMap<>();
    }

    @Override
    public Heap push(String id, Object returnValue, int counter)
    {
        this.contents.put(id, new Counter(returnValue, counter));
        return this;
    }

    @Override
    public boolean has(String id)
    {
        return this.contents.containsKey(id);
    }

    @Override
    public synchronized Object peek(String id)
    {
        if (this.contents.containsKey(id))
            return this.contents.get(id).getValue();
        return null;
    }

    @Override
    public synchronized Object consume(String id)
    {
        if (this.contents.containsKey(id))
        {
            Counter counter = this.contents.get(id).decrement();
            if (counter.getCount() == 0)
                this.contents.remove(id);
            return counter.getValue();
        }
        return null;
    }
}
