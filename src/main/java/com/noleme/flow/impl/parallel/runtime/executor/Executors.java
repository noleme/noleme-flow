package com.noleme.flow.impl.parallel.runtime.executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/10
 */
public final class Executors
{
    private Executors()
    {
    }

    public static ExecutorService newFixedThreadPool(int nThreads)
    {
        return new ThrowingThreadPoolExecutor(
            nThreads,
            nThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>()
        );
    }
}
