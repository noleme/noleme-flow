package com.noleme.flow.io.input;

import com.noleme.flow.actor.extractor.Extractor;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public final class InputExtractor<T> implements Extractor<T>
{
    private final Key<T> key;

    public InputExtractor(Key<T> key)
    {
        this.key = key;
    }

    @Override
    public T extract()
    {
        throw new IllegalStateException("The InputExtractor is a special-case extractor and its extract() method is not expected to be called.");
    }

    public Key<T> getKey()
    {
        return this.key;
    }
}
