package com.noleme.flow.impl.pipeline.runtime.heap;

import com.noleme.flow.actor.generator.Generator;
import com.noleme.flow.io.input.Key;
import com.noleme.flow.io.output.Output;
import com.noleme.flow.stream.StreamGenerator;

import java.util.Collection;

/**
 * @author Pierre Lecerf (pierre.lecerf@gmail.com) on 23/07/2015.
 */
@SuppressWarnings("rawtypes")
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

    /* Stream related methods */

    /**
     *
     * @param node
     * @return
     */
    Generator getStreamGenerator(StreamGenerator node);

    /**
     *
     * @param node
     * @return
     */
    long getNextStreamOffset(StreamGenerator node);

    /**
     *
     * @param id
     * @param offset
     * @param returnValue
     * @param counter
     * @return
     */
    Heap push(String id, long offset, Object returnValue, int counter);

    /**
     *
     * @param id
     * @param offset
     * @return boolean
     */
    boolean has(String id, long offset);

    /**
     *
     * @param id
     * @param offset
     * @return Object
     */
    Object peek(String id, long offset);

    /**
     *
     * @param id
     * @param offset
     * @return
     */
    Object consume(String id, long offset);

    /**
     *
     * @param id
     * @return
     */
    Collection<Object> consumeAll(String id);

    /**
     *
     * @param key
     * @return
     */
    boolean hasInput(Key<?> key);

    /**
     *
     * @param key
     * @return
     */
    <T> T getInput(Key<T> key);

    /**
     *
     * @param identifier
     * @param value
     * @return
     */
    Heap setOutput(String identifier, Object value);

    /**
     *
     * @return
     */
    Output getOutput();
}
