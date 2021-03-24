package com.noleme.flow.io.output;

import com.noleme.flow.Sink;
import com.noleme.flow.actor.loader.BlackHole;
import com.noleme.flow.actor.loader.Loader;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public final class Recipient <I> extends Sink<I>
{
    private static final Loader blackHole = new BlackHole();
    private final String identifier;

    public Recipient(String identifier)
    {
        //noinspection unchecked
        super(blackHole);
        this.identifier = identifier;
    }

    public String getIdentifier()
    {
        return this.identifier;
    }
}
