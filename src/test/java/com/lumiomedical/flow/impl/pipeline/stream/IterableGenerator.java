package com.lumiomedical.flow.impl.pipeline.stream;

import com.lumiomedical.flow.actor.generator.Generator;

import java.util.Iterator;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class IterableGenerator<T> implements Generator<T>
{
    private final Iterator<T> iterator;

    /**
     *
     * @param iterable
     */
    public IterableGenerator(Iterable<T> iterable)
    {
        this.iterator = iterable.iterator();
    }

    @Override
    public boolean hasNext()
    {
        return this.iterator.hasNext();
    }

    @Override
    public T generate()
    {
        return this.iterator.hasNext() ? this.iterator.next() : null;
    }
}
