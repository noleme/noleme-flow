package com.lumiomedical.flow.compiler;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public class CompilationException extends Exception
{
    /**
     *
     * @param message
     */
    public CompilationException(String message)
    {
        super(message);
    }

    /**
     *
     * @param message
     * @param cause
     */
    public CompilationException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
