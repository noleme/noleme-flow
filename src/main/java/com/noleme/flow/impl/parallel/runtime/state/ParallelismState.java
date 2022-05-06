package com.noleme.flow.impl.parallel.runtime.state;

import com.noleme.flow.impl.pipeline.runtime.node.WorkingNode;
import com.noleme.flow.stream.StreamGenerator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Pierre LECERF (pierre@noleme.com)
 * Created on 20/06/2021
 */
@SuppressWarnings("rawtypes")
public class ParallelismState
{
    /* Parallelism is evaluated with regards to the generators themselves */
    private final Map<WorkingNode<StreamGenerator>, ParallelismCounter> counters = new ConcurrentHashMap<>();
    private final RRWLock lock = new RRWLock();

    /**
     *
     * @param generator
     * @return
     */
    public boolean has(WorkingNode<StreamGenerator> generator)
    {
        try {
            this.lock.read.lock();
            return this.counters.containsKey(generator);
        }
        finally {
            this.lock.read.unlock();
        }
    }

    /**
     *
     * @param generator
     * @return
     */
    public boolean hasReachedMax(StreamGenerator generator)
    {
        try {
            this.lock.read.lock();

            return this.counters.containsKey(generator)
                && this.counters.get(generator).count() >= generator.getMaxParallelism()
            ;
        }
        finally {
            this.lock.read.unlock();
        }
    }

    /**
     *
     * @param generator
     * @return
     */
    public boolean isIdle(WorkingNode<StreamGenerator> generator)
    {
        try {
            this.lock.read.lock();

            return this.counters.containsKey(generator)
                && this.counters.get(generator).count() == 0
            ;
        }
        finally {
            this.lock.read.unlock();
        }
    }

    /**
     *
     * @param generator
     * @return
     */
    public int increase(WorkingNode<StreamGenerator> generator)
    {
        try {
            this.lock.write.lock();
            return this.getOrCreateParallelism(generator).increment();
        }
        finally {
            this.lock.write.unlock();
        }
    }

    /**
     *
     * @param generator
     * @return
     */
    public int decrease(WorkingNode<StreamGenerator> generator)
    {
        try {
            this.lock.write.lock();
            return this.getOrCreateParallelism(generator).decrement();
        }
        finally {
            this.lock.write.unlock();
        }
    }

    /**
     *
     * @param generator
     * @return
     */
    private ParallelismCounter getOrCreateParallelism(WorkingNode<StreamGenerator> generator)
    {
        if (!this.counters.containsKey(generator))
            this.counters.put(generator, new ParallelismCounter());
        return this.counters.get(generator);
    }

    private static class ParallelismCounter
    {
        private int counter = 0;

        public int count() { return this.counter; }
        public int increment()
        {
            synchronized (this) {
                return ++this.counter;
            }
        }
        public int decrement()
        {
            synchronized (this) {
                return --this.counter;
            }
        }
    }
}
