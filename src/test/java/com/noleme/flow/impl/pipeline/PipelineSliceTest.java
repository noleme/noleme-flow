package com.noleme.flow.impl.pipeline;

import com.noleme.flow.Flow;
import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.compiler.RunException;
import com.noleme.flow.impl.pipeline.stream.IterableGenerator;
import com.noleme.flow.io.output.Recipient;
import com.noleme.flow.slice.TestPipe;
import com.noleme.flow.slice.TestSink;
import com.noleme.flow.slice.TestSource;
import com.noleme.flow.slice.TestStreamSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author Pierre Lecerf (pierre.lecerf@illuin.tech)
 */
public class PipelineSliceTest
{
    @Test
    public void sliceTest()
    {
        Assertions.assertDoesNotThrow(() -> {
            var f0 = Flow
                .from(new TestSource())
                .pipe(input -> input + "_append")
            ;
            Flow.runAsPipeline(f0);
        });

        Assertions.assertDoesNotThrow(() -> {
            var f1 = Flow
                .from(() -> "some_input")
                .pipe(new TestPipe())
            ;
            Flow.runAsPipeline(f1);
        });

        Assertions.assertDoesNotThrow(() -> {
            var f2 = Flow
                .from(() -> "some_input")
                .pipe(input -> input + "_append")
                .sink(new TestSink())
            ;
            Flow.runAsPipeline(f2);
        });

        Assertions.assertDoesNotThrow(() -> {
            var f3 = Flow
                .from(new TestSource())
                .pipe(new TestPipe())
                .sink(new TestSink())
            ;
            Flow.runAsPipeline(f3);
        });
    }

    @Test
    public void streamSliceTest()
    {
        Assertions.assertDoesNotThrow(() -> {
            var f0 = Flow
                .from(new TestStreamSource())
                .pipe(new TestPipe())
                .sink(new TestSink())
            ;
            Flow.runAsPipeline(f0);
        });

        Assertions.assertDoesNotThrow(() -> {
            var f1 = Flow
                .from(() -> List.of("abc", "def", "ghi", "klm", "nop"))
                .stream(IterableGenerator::new)
                .pipe(new TestPipe())
                .sink(new TestSink())
            ;
            Flow.runAsPipeline(f1);
        });
    }

    @Test
    public void testTypedFlow_asLead() throws CompilationException, RunException
    {
        Recipient<String> f0 = Flow
            .from(new TestSource())
            .pipe(new TestPipe()).asLead()
            .collect()
        ;

        var output = Flow.runAsPipeline(f0);

        Assertions.assertEquals("abc_def_ghi_klm", output.get(f0));
    }

    @Test
    public void testTypedFlow_asStream() throws CompilationException, RunException
    {
        Recipient<String> f0 = Flow
            .from(new TestStreamSource())
            .pipe(new TestPipe()).asStream()
            .accumulate(Object::toString)
            .collect()
        ;

        var output = Flow.runAsPipeline(f0);

        Assertions.assertEquals("[abc:_ghi_klm, def:_ghi_klm, ghi:_ghi_klm, klm:_ghi_klm, nop:_ghi_klm]", output.get(f0));
    }
}
