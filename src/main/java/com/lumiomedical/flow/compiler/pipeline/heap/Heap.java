package com.lumiomedical.flow.compiler.pipeline.heap;

/**
 * @author Pierre Lecerf (pierre.lecerf@gmail.com) on 23/07/2015.
 */
public interface Heap
{
    /**
     *
     * @param id String
     * @param returnValue Object
     * @param counter
     */
    Heap push(String id, Object returnValue, int counter);

    /**
     *
     * @param id String
     * @return boolean
     */
    boolean has(String id);

    /**
     *
     * @param id String
     * @return Object
     */
    Object peek(String id);

    /**
     *
     * @param id
     * @return
     */
    Object consume(String id);
}
