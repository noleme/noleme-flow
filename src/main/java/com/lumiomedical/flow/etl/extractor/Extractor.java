package com.lumiomedical.flow.etl.extractor;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/26
 */
public interface Extractor<T>
{
    /**
     *
     * @return
     * @throws ExtractionException
     */
    T extract() throws ExtractionException;
}
