package com.noleme.flow.slice;

import com.noleme.flow.FlowOut;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class TestPipe extends PipeSlice<String, String>
{
    @Override
    public FlowOut<String> out(FlowOut<String> upstream)
    {
        return upstream
            .pipe(v -> v + "_ghi")
            .pipe(v -> v + "_klm")
        ;
    }
}
