package com.noleme.flow.compiler;

import com.noleme.flow.io.input.Input;
import com.noleme.flow.io.output.Output;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/02
 */
public interface FlowRuntime
{
    /**
     *
     * @param input
     * @throws RunException
     */
    Output run(Input input) throws RunException;

    /**
     *
     * @throws RunException
     */
    default Output run() throws RunException
    {
        return this.run(Input.empty());
    }
}
