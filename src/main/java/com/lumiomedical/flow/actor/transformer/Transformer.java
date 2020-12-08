package com.lumiomedical.flow.actor.transformer;

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
}
