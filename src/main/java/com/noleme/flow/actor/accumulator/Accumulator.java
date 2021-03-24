package com.noleme.flow.actor.accumulator;

import java.util.Collection;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/03
 */
public interface Accumulator<I, O>
{
    /**
     * This method should transform a collection of inputs resulting from stream executions into a unified output.
     *
     * @param input
     * @return
     * @throws AccumulationException
     */
    O accumulate(Collection<I> input) throws AccumulationException;
}
