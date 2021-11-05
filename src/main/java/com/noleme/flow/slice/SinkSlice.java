package com.noleme.flow.slice;

import com.noleme.flow.CurrentIn;
import com.noleme.flow.CurrentOut;
import com.noleme.flow.annotation.Experimental;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
@Experimental
public abstract class SinkSlice<I>
{
    public abstract CurrentIn<?> out(CurrentOut<I> upstream);
}
