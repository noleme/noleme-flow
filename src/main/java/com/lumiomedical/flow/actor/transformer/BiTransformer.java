package com.lumiomedical.flow.actor.transformer;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/03
 */
public interface BiTransformer<I1, I2, O>
{
    /**
     *
     * @param input1
     * @param input2
     * @return
     * @throws TransformationException
     */
    O transform(I1 input1, I2 input2) throws TransformationException;
}
