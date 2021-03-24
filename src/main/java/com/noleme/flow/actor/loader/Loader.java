package com.noleme.flow.actor.loader;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/01
 */
public interface Loader<T>
{
    /**
     *
     * @param input
     * @throws LoadingException
     */
    void load(T input) throws LoadingException;
}
