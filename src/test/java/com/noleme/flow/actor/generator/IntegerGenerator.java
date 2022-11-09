package com.noleme.flow.actor.generator;

import java.util.function.Function;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/08
 */
public class IntegerGenerator implements Generator<Integer>
{
    private final int max;
    private final Function<Integer, Integer> func;
    private int i;

    public IntegerGenerator(int start, int max)
    {
        this(start, max, i -> ++i);
    }

    public IntegerGenerator(int start, int max, Function<Integer, Integer> func)
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
    public Integer generate()
    {
        int value = this.i;
        this.i = this.func.apply(this.i);
        return value;
    }
}
