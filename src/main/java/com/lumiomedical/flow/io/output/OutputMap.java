package com.lumiomedical.flow.io.output;

import com.lumiomedical.flow.compiler.RunException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public class OutputMap implements WriteableOutput
{
    private final Map<String, Object> values;

    public OutputMap()
    {
        this.values = new HashMap<>();
    }

    @Override
    public <T> boolean has(Recipient<T> recipient)
    {
        return this.has(recipient.getIdentifier());
    }

    @Override
    public boolean has(String identifier)
    {
        return this.values.containsKey(identifier);
    }

    @Override
    public <T> T get(Recipient<T> recipient)
    {
        if (!this.values.containsKey(recipient.getIdentifier()))
            return null;

        Object value = this.values.get(recipient.getIdentifier());

        if (value == null)
            return null;

        //noinspection unchecked
        return (T) value;
    }

    @Override
    public <T> T get(String identifier, Class<T> type)
    {
        if (!this.values.containsKey(identifier))
            return null;

        Object value = this.values.get(identifier);

        if (value == null)
            return null;
        if (!type.isAssignableFrom(value.getClass()))
            throw new ClassCastException("A sample was found for identifier "+identifier+" but it was of a different type "+value.getClass().getName()+" (requested type was "+type.getName()+")");

        //noinspection unchecked
        return (T) value;
    }

    @Override
    public Object get(String identifier)
    {
        return this.values.get(identifier);
    }

    @Override
    public WriteableOutput set(String identifier, Object value)
    {
        this.values.put(identifier, value);
        return this;
    }
}
