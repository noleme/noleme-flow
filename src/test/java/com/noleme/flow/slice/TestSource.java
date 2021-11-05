package com.noleme.flow.slice;

import com.noleme.flow.Flow;
import com.noleme.flow.CurrentOut;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class TestSource extends SourceSlice<String>
{
    @Override
    public CurrentOut<String> out()
    {
        return Flow
            .from(() -> "abc")
            .pipe(v -> v + "_def")
        ;
    }
}
