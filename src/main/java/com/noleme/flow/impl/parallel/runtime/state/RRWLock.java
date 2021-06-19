package com.noleme.flow.impl.parallel.runtime.state;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Pierre Lecerf (pierre@noleme.com)
 */
public final class RRWLock
{
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    public final Lock read = this.lock.readLock();
    public final Lock write = this.lock.writeLock();
}
