package com.lumiomedical.flow.io.input;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public class InputMap implements Input
{
    private final Map<String, Object> values;

    InputMap()
    {
        this.values = new HashMap<>();
    }

    @Override
    public <T> InputMap and(String identifier, T value)
    {
        this.values.put(identifier, value);
        return this;
    }

    @Override
    public boolean has(String identifier)
    {
        return this.values.containsKey(identifier);
    }

    @Override
    public Object get(String identifier)
    {
        return this.values.get(identifier);
    }
}
