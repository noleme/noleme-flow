package com.noleme.flow.impl.parallel.runtime.heap;

import com.noleme.flow.impl.parallel.runtime.state.RRWLock;
import com.noleme.flow.impl.pipeline.runtime.heap.Counter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/11
 */
public class ConcurrentCounterContainer
{
    private final RRWLock lock = new RRWLock();
    private final Map<Long, Counter> data;

    public ConcurrentCounterContainer()
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
        try {
            this.lock.read.lock();
            return this.data.get(offset);
        }
        finally {
            this.lock.read.unlock();
        }
    }

    /**
     *
     * @param offset
     * @return
     */
    public Counter remove(long offset)
    {
        try {
            this.lock.write.lock();
            return this.data.remove(offset);
        }
        finally {
            this.lock.write.unlock();
        }
    }

    /**
     *
     * @return
     */
    public Stream<Counter> stream()
    {
        try {
            this.lock.read.lock();
            return this.data.values().stream();
        }
        finally {
            this.lock.read.unlock();
        }
    }

    /**
     *
     * @return
     */
    public int removeConsumed()
    {
        try {
            this.lock.write.lock();
            this.data.values().removeIf(counter -> counter.getCount() == 0);

            return this.data.size();
        }
        finally {
            this.lock.write.unlock();
        }
    }

    /**
     *
     * @param offset
     * @param counter
     */
    public void set(long offset, Counter counter)
    {
        try {
            this.lock.write.lock();
            this.data.put(offset, counter);
        }
        finally {
            this.lock.write.unlock();
        }
    }
}
