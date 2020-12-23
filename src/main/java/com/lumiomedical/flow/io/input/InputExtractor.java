package com.lumiomedical.flow.io.input;

import com.lumiomedical.flow.actor.extractor.Extractor;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public final class InputExtractor <T> implements Extractor<T>
{
    private final String identifier;

    public InputExtractor(String identifier)
    {
        this.identifier = identifier;
    }

    @Override
    public T extract()
    {
        throw new IllegalStateException("The InputExtractor is a special-case extractor and its extract() method is not expected to be called.");
    }

    public String getIdentifier()
    {
        return this.identifier;
    }
}
