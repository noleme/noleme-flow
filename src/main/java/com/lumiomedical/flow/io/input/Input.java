package com.lumiomedical.flow.io.input;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public interface Input
{
    Input emptyInput = new InputEmpty();

    /**
     *
     * @param <T>
     * @return
     */
    static <T> Input empty()
    {
        return emptyInput;
    }

    /**
     *
     * @param identifier
     * @param value
     * @param <T>
     * @return
     */
    static <T> Input of(String identifier, T value)
    {
        return new InputMap().and(identifier, value);
    }

    /**
     *
     * @param identifier
     * @param value
     * @param <T>
     * @return
     */
    <T> InputMap and(String identifier, T value);

    /**
     *
     * @param identifier
     * @return
     */
    boolean has(String identifier);

    /**
     *
     * @param identifier
     * @return
     */
    Object get(String identifier);
}
