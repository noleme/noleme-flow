package com.noleme.flow.actor.extractor;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/26
 */
public interface Extractor<T>
{
    /**
     *
     * @return
     * @throws Exception
     */
    T extract() throws Exception;
}
