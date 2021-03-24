package com.noleme.flow.impl.pipeline.runtime.heap;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/11
 */
public class CounterContainer
{
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();
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
            this.readLock.lock();
            return this.data[offset];
        }
        finally {
            this.readLock.unlock();
        }
    }

    /**
     *
     * @return
     */
    public Stream<Counter> stream()
    {
        try {
            this.readLock.lock();
            return Stream.of(this.data)
                .filter(Objects::nonNull)
            ;
        }
        finally {
            this.readLock.unlock();
        }
    }

    /**
     *
     */
    public void removeConsumed()
    {
        try {
            this.writeLock.lock();
            for (int offset = 0 ; offset < this.data.length ; ++offset)
            {
                if (this.data[offset] == null)
                    continue;

                if (this.data[offset].getCount() == 0)
                    this.data[offset] = null;
            }
        }
        finally {
            this.writeLock.unlock();
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
            this.writeLock.lock();
            this.data[offset] = counter;
        }
        finally {
            this.writeLock.unlock();
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
            this.writeLock.lock();

            if (offset < this.data.length)
                return;

            int targetSize = offset + 1 + OFFSET_GROWTH;

            if (offset + 1 > this.data.length)
                this.data = Arrays.copyOf(this.data, targetSize);
        }
        finally {
            this.writeLock.unlock();
        }
    }
}
