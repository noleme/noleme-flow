package com.noleme.flow.actor.generator;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
@Deprecated
public class GenerationException extends Exception
{
    /**
     *
     * @param message
     */
    public GenerationException(String message)
    {
        super(message);
    }

    /**
     *
     * @param message
     * @param cause
     */
    public GenerationException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
