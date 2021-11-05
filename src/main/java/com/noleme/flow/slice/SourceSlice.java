package com.noleme.flow.slice;

import com.noleme.flow.CurrentOut;
import com.noleme.flow.annotation.Experimental;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
@Experimental
public abstract class SourceSlice<O>
{
    public abstract CurrentOut<O> out();
}
