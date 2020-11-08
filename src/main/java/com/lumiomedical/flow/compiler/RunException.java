package com.lumiomedical.flow.compiler;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public class RunException extends Exception
{
    /**
     *
     * @param message
     */
    public RunException(String message)
    {
        super(message);
    }

    /**
     *
     * @param message
     * @param cause
     */
    public RunException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
