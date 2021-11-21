package com.noleme.flow.impl.pipeline.runtime.heap;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/11
 */
public class CounterContainer
{
    private final Map<Long, Counter> data;

    public CounterContainer()
    {
        this.data = new HashMap<>();
    }

    /**
     *
     * @param offset
     * @return
     */
    public Counter get(long offset)
    {
        return this.data.get(offset);
    }

    /**
     *
     * @param offset
     * @return
     */
    public Counter remove(long offset)
    {
        return this.data.remove(offset);
    }

    /**
     *
     * @return
     */
    public Stream<Counter> stream()
    {
        return this.data.values().stream();
    }

    /**
     *
     * @return
     */
    public int removeConsumed()
    {
        this.data.values().removeIf(counter -> counter.getCount() == 0);

        return this.data.size();
    }

    /**
     *
     * @param offset
     * @param counter
     */
    public void set(long offset, Counter counter)
    {
        this.data.put(offset, counter);
    }
}
