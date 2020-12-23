package com.lumiomedical.flow.interruption;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/22
 */
public final class InterruptionException extends RuntimeException
{
    private static final InterruptionException singleton = new InterruptionException();

    /**
     * @throws InterruptionException
     */
    public static InterruptionException interrupt() throws InterruptionException
    {
        return InterruptionException.singleton;
    }

    private InterruptionException()
    {
        super("Flow branch interruption request", null, false, false);
    }
}
