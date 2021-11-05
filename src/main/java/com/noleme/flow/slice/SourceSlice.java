package com.noleme.flow.slice;

import com.noleme.flow.FlowOut;
import com.noleme.flow.annotation.Experimental;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
@Experimental
public abstract class SourceSlice<O>
{
    public abstract FlowOut<O> out();
}
