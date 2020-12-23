package com.lumiomedical.flow.io.output;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public interface Output
{
    /**
     *
     * @param recipient
     * @param <T>
     * @return
     */
    <T> boolean has(Recipient<T> recipient);

    /**
     *
     * @param identifier
     * @return
     */
    boolean has(String identifier);

    /**
     *
     * @param recipient
     * @param <T>
     * @return
     */
    <T> T get(Recipient<T> recipient);

    /**
     *
     * @param identifier
     * @param type
     * @param <T>
     * @return
     */
    <T> T get(String identifier, Class<T> type);

    /**
     *
     * @param identifier
     * @return
     */
    Object get(String identifier);
}
