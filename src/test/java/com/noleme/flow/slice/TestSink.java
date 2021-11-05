package com.noleme.flow.slice;

import com.noleme.flow.FlowIn;
import com.noleme.flow.FlowOut;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class TestSink extends SinkSlice<String>
{
    @Override
    public FlowIn<?> out(FlowOut<String> upstream)
    {
        return upstream
            .pipe(v -> v + "_nop")
            .sink(v -> {})
        ;
    }
}
