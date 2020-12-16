package com.lumiomedical.flow.compiler;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/06
 */
public final class NotImplementedException extends RuntimeException
{
    /**
     *
     */
    public NotImplementedException()
    {
        super();
    }

    /**
     *
     * @param message
     */
    public NotImplementedException(String message)
    {
        super(message);
    }
}
