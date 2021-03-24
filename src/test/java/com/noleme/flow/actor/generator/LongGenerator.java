package com.noleme.flow.actor.generator;

import java.util.function.Function;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/08
 */
public class LongGenerator implements Generator<Long>
{
    private final long max;
    private final Function<Long, Long> func;
    private long i;

    public LongGenerator(long start, long max, Function<Long, Long> func)
    {
        this.i = start;
        this.max = max;
        this.func = func;
    }

    @Override
    public boolean hasNext()
    {
        return this.i < this.max;
    }

    @Override
    public Long generate()
    {
        long value = this.i;
        this.i = this.func.apply(this.i);
        return value;
    }
}
