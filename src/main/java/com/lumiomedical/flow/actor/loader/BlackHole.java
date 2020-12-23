package com.lumiomedical.flow.actor.loader;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/23
 */
public class BlackHole <T> implements Loader<T>
{
    @Override
    public void load(T input)
    {
        //nothing
    }
}
