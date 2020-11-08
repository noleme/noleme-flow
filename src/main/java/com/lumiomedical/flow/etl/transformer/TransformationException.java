package com.lumiomedical.flow.etl.transformer;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/26
 */
public class TransformationException extends Exception
{
    /**
     *
     * @param message
     */
    public TransformationException(String message)
    {
        super(message);
    }

    /**
     *
     * @param message
     * @param cause
     */
    public TransformationException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
