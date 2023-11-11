package com.noleme.flow.impl.parallel;

import com.noleme.flow.Flow;
import com.noleme.flow.FlowAssertion;
import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.compiler.RunException;
import com.noleme.flow.impl.pipeline.stream.IterableGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class ParallelStreamCascadeTest
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

        Flow.runAsParallel(flow);

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
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .accumulate()
            .sink(i -> assertion.activate())
        ;

        Flow.runAsParallel(flow);

        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(2, assertion.getActivationCount());
    }

    @Test
    void testStreamDoubleAggregation() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .from(() -> List.of(List.of(1, 2, 3), List.of(4, 5, 6)))
            .stream(IterableGenerator::new)
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .accumulate()
            //.accumulate()
            .sink(i -> assertion.activate())
        ;

        Flow.runAsParallel(flow);

        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(5, assertion.getActivationCount());
    }
}
