package com.lumiomedical.flow.actor.transformer;

import com.lumiomedical.flow.actor.extractor.ExtractionException;
import com.lumiomedical.flow.actor.extractor.Extractor;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/02/26
 */
public interface Transformer<I, O>
{
    /**
     *
     * @param input
     * @return
     * @throws TransformationException
     */
    O transform(I input) throws TransformationException;

    /**
     * Allows the use of a Transformer contract as an Extractor provided a given input.
     *
     * @param input
     * @return
     */
    default Extractor<O> asExtractor(I input)
    {
        return () -> {
            try {
                return this.transform(input);
            }
            catch (TransformationException e) {
                throw new ExtractionException(e.getMessage(), e);
            }
        };
    }
}
