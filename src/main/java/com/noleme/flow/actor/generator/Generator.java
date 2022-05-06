package com.noleme.flow.actor.generator;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public interface Generator <O>
{
    /**
     * This method should return true if the generator is active, false otherwise.
     * An active generator still has elements to provide, an inactive generator effectively means the stream is dried up.
     *
     * @return true if the generator is still active, false otherwise
     */
    boolean hasNext();

    /**
     * This method should return the next element of the stream each time it is called or null if it has reached the end.
     *
     * @throws Exception
     */
    O generate() throws Exception;
}
