package com.noleme.flow.slice;

import com.noleme.flow.Flow;
import com.noleme.flow.FlowOut;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class TestSource extends SourceSlice<String>
{
    @Override
    public FlowOut<String> out()
    {
        return Flow
            .from(() -> "abc")
            .pipe(v -> v + "_def")
        ;
    }
}
