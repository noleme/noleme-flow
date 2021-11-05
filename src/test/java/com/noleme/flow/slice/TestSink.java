package com.noleme.flow.slice;

import com.noleme.flow.CurrentIn;
import com.noleme.flow.CurrentOut;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class TestSink extends SinkSlice<String>
{
    @Override
    public CurrentIn<?> out(CurrentOut<String> upstream)
    {
        return upstream
            .pipe(v -> v + "_nop")
            .sink(v -> {})
        ;
    }
}
