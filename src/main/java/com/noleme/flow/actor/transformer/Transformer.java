package com.noleme.flow.actor.transformer;

import com.noleme.flow.actor.extractor.Extractor;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/26
 */
@FunctionalInterface
public interface Transformer<I, O>
{
    /**
     *
     * @param input
     * @return
     * @throws Exception
     */
    O transform(I input) throws Exception;

    /**
     * Allows the use of a Transformer contract as an Extractor provided a given input.
     *
     * @param input
     * @return
     */
    default Extractor<O> asExtractor(I input)
    {
        return () -> this.transform(input);
    }
}
