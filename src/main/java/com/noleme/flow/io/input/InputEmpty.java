package com.noleme.flow.io.input;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
class InputEmpty implements Input
{
    @Override
    public <T> InputMap and(Key<T> key, T value)
    {
        throw new RuntimeException("The 'and' method cannot be called on an empty input.");
    }

    @Override
    public boolean has(Key<?> key)
    {
        return false;
    }

    @Override
    public <T> T get(Key<T> key)
    {
        return null;
    }
}
