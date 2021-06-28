package com.noleme.flow.impl.pipeline.runtime.heap;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
public class Counter
{
    private final Object value;
    private int count;

    public Counter(Object value, int counter)
    {
        this.value = value;
        this.count = counter;
    }

    /**
     *
     * @return
     */
    synchronized public Counter decrement()
    {
        if (this.count > 0)
            this.count--;
        return this;
    }

    /**
     *
     * @return
     */
    public Object getValue()
    {
        return this.value;
    }

    /**
     *
     * @return
     */
    public int getCount()
    {
        return this.count;
    }
}
