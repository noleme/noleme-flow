package com.noleme.flow.actor.loader;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
@Deprecated
public class LoadingException extends Exception
{
    /**
     *
     * @param message
     */
    public LoadingException(String message)
    {
        super(message);
    }

    /**
     *
     * @param message
     * @param cause
     */
    public LoadingException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
