package com.noleme.flow.io.input;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
class InputEmpty implements Input
{
    @Override
    public <T> InputMap and(String identifier, T value)
    {
        throw new RuntimeException("The 'and' method cannot be called on an empty input.");
    }

    @Override
    public boolean has(String identifier)
    {
        return false;
    }

    @Override
    public Object get(String identifier)
    {
        return null;
    }
}
