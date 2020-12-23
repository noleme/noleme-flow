package com.lumiomedical.flow.interruption;

import com.lumiomedical.flow.actor.transformer.Transformer;

import java.util.function.Predicate;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/22
 */
public class Interruption <I> implements Transformer<I, I>
{
    private final Predicate<I> predicate;

    /**
     *
     * @param predicate
     */
    public Interruption(Predicate<I> predicate)
    {
        this.predicate = predicate;
    }

    public Interruption()
    {
        this(null);
    }

    @Override
    public I transform(I input)
    {
        if (this.predicate == null || this.predicate.test(input))
            throw InterruptionException.interrupt();
        return input;
    }
}
