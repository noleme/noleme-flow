package com.noleme.flow.impl.pipeline.runtime.heap;

import com.noleme.flow.impl.parallel.runtime.state.RRWLock;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/11
 */
public class CounterContainer
{
    private final RRWLock lock = new RRWLock();
    private Counter[] data;
    private static final int OFFSET_GROWTH = 10;

    public CounterContainer()
    {
        this.data = new Counter[OFFSET_GROWTH];
    }

    /**
     *
     * @param offset
     * @return
     */
    public Counter get(int offset)
    {
        this.ensureOffset(offset);

        try {
            this.lock.read.lock();
            return this.data[offset];
        }
        finally {
            this.lock.read.unlock();
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
            return Stream.of(this.data)
                .filter(Objects::nonNull)
            ;
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

            int remaining = 0;
            for (int offset = 0 ; offset < this.data.length ; ++offset)
            {
                if (this.data[offset] == null)
                    continue;

                if (this.data[offset].getCount() == 0)
                    this.data[offset] = null;
                else
                    ++remaining;
            }

            return remaining;
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
    public void set(int offset, Counter counter)
    {
        this.ensureOffset(offset);

        try {
            this.lock.write.lock();
            this.data[offset] = counter;
        }
        finally {
            this.lock.write.unlock();
        }
    }

    /**
     *
     * @param offset
     */
    private void ensureOffset(int offset)
    {
        if (offset < this.data.length)
            return;

        try {
            this.lock.write.lock();

            if (offset < this.data.length)
                return;

            int targetSize = offset + 1 + OFFSET_GROWTH;

            if (offset + 1 > this.data.length)
                this.data = Arrays.copyOf(this.data, targetSize);
        }
        finally {
            this.lock.write.unlock();
        }
    }
}
