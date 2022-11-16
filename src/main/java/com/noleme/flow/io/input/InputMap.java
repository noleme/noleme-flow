package com.noleme.flow.io.input;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public class InputMap implements Input
{
    private final Map<Key<?>, Object> values;

    InputMap()
    {
        this.values = new HashMap<>();
    }

    @Override
    public <T> InputMap and(Key<T> key, T value)
    {
        this.values.put(key, key.validate(value));
        return this;
    }

    @Override
    public boolean has(Key<?> key)
    {
        return this.values.containsKey(key);
    }

    @Override
    public <T> T get(Key<T> key)
    {
        /* Type is checked upon insertion */
        //noinspection unchecked
        return (T) this.values.get(key);
    }
}
