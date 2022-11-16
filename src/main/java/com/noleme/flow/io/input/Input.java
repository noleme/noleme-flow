package com.noleme.flow.io.input;

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

    static <T> Key<T> key(Class<T> type)
    {
        return new Key<>(type);
    }

    static Key<Object> key(String identifier)
    {
        return new Key<>(Object.class, identifier);
    }

    /**
     *
     * @param key
     * @param value
     * @return
     * @param <T>
     */
    static <T> Input of(Key<T> key, T value)
    {
        return new InputMap().and(key, value);
    }

    /**
     * @see #of(Key, Object)
     */
    static <T> Input of(String identifier, T value)
    {
        return of(createUntypedKey(identifier), value);
    }

    /**
     *
     * @param key
     * @param value
     * @return
     * @param <T>
     */
    <T> InputMap and(Key<T> key, T value);

    /**
     * @see #and(Key, Object)
     */
    default <T> InputMap and(String identifier, T value)
    {
        return this.and(createUntypedKey(identifier), value);
    }

    /**
     *
     * @param key
     * @return
     */
    boolean has(Key<?> key);

    /**
     * @see #has(Key)
     */
    default boolean has(String identifier)
    {
        return this.has(createUntypedKey(identifier));
    }

    /**
     *
     * @param key
     * @return
     * @param <T>
     */
    <T> T get(Key<T> key);

    /**
     * @see #get(Key)
     */
    default Object get(String identifier)
    {
        return this.get(createUntypedKey(identifier));
    }

    private static Key<Object> createUntypedKey(String identifier)
    {
        return new Key<>(Object.class, identifier);
    }
}
