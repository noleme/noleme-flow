package com.noleme.flow.actor.loader;

import com.noleme.flow.actor.Silent;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
@Silent
public final class End<T> implements Loader<T>
{
    @Override
    public void load(T input) { /* nothing */ }
}
