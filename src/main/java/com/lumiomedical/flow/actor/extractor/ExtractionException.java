package com.lumiomedical.flow.actor.extractor;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/26
 */
public class ExtractionException extends Exception
{
    /**
     *
     * @param message
     */
    public ExtractionException(String message)
    {
        super(message);
    }

    /**
     *
     * @param message
     * @param cause
     */
    public ExtractionException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
