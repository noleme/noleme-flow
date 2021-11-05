package com.noleme.flow.slice;

import com.noleme.flow.CurrentOut;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class TestPipe extends PipeSlice<String, String>
{
    @Override
    public CurrentOut<String> out(CurrentOut<String> upstream)
    {
        return upstream
            .pipe(v -> v + "_ghi")
            .pipe(v -> v + "_klm")
        ;
    }
}
