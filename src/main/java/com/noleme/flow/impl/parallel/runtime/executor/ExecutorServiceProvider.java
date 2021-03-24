package com.noleme.flow.impl.parallel.runtime.executor;

import java.util.concurrent.ExecutorService;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/06
 */
public interface ExecutorServiceProvider
{
    /**
     *
     * @return
     */
    ExecutorService provide();
}
