package com.noleme.flow.slice;

import com.noleme.flow.Flow;
import com.noleme.flow.impl.pipeline.stream.IterableGenerator;
import com.noleme.flow.stream.StreamOut;

import java.util.List;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class TestStreamSource extends SourceSlice<String>
{
    @Override
    public StreamOut<String> out()
    {
        return Flow
            .from(() -> List.of("abc", "def", "ghi", "klm", "nop"))
            .stream(IterableGenerator::new)
            .pipe(str -> str + ":")
        ;
    }
}
