package com.noleme.flow.impl.pipeline;

import com.noleme.flow.Flow;
import com.noleme.flow.FlowAssertion;
import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.compiler.RunException;
import com.noleme.flow.impl.pipeline.stream.IterableGenerator;
import com.noleme.flow.io.output.Output;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class PipelineStreamCascadeTest
{
    @Test
    void testStream() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .from(() -> List.of(List.of(1, 2, 3), List.of(4, 5, 6)))
            .stream(IterableGenerator::new)
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .sink(i -> assertion.activate())
        ;

        Flow.runAsPipeline(flow);

        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(6, assertion.getActivationCount());
    }

    @Test
    void testStreamAggregation() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .from(() -> List.of(List.of(1, 2, 3), List.of(4, 5, 6)))
            .stream(IterableGenerator::new)
            .driftSink(i -> assertion.activate()) // 2
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .driftSink(System.out::println)
            .driftSink(i -> assertion.activate()) // 3 + 3
            .accumulate()
            .driftSink(i -> assertion.activate()) // 2
        ;

        Flow.runAsPipeline(flow);

        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(10, assertion.getActivationCount());
    }

    @Test
    void testStreamDoubleAggregation() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .from(() -> List.of(List.of(1, 2, 3), List.of(4, 5, 6)))
            .stream(IterableGenerator::new)
            .driftSink(System.out::println)
            .driftSink(i -> assertion.activate()) // 2
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .driftSink(System.out::println)
            .driftSink(i -> assertion.activate()) // 3 + 3
            .accumulate().name("inner-accumulator").asStream()
            .driftSink(System.out::println)
            .driftSink(i -> assertion.activate()) // 2
            .accumulate().name("outer-accumulator").asFlow()
            .driftSink(System.out::println)
            .driftSink(i -> assertion.activate()) // 1
            .collect()
        ;

        Output output = Flow.runAsPipeline(flow);

        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(11, assertion.getActivationCount());
        Assertions.assertEquals(List.of(List.of(2, 3, 4), List.of(5, 6, 7)), output.get(flow));
    }
}
