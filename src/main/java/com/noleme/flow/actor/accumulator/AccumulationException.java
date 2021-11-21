package com.noleme.flow.actor.accumulator;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
@Deprecated
public class AccumulationException extends Exception
{
    /**
     *
     * @param message
     */
    public AccumulationException(String message)
    {
        super(message);
    }

    /**
     *
     * @param message
     * @param cause
     */
    public AccumulationException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
