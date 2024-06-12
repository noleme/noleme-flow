package com.noleme.flow.actor.transformer;

import com.noleme.flow.actor.Silent;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 * Created on 2020/11/18
 */
@Silent
public final class Beam<I> implements Transformer<I, I>
{
    @Override
    public I transform(I input) { /* nothing */ return input; }
}
