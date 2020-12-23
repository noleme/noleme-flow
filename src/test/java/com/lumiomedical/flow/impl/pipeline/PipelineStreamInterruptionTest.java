package com.lumiomedical.flow.impl.pipeline;

import com.lumiomedical.flow.Flow;
import com.lumiomedical.flow.FlowAssertion;
import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.RunException;
import com.lumiomedical.flow.impl.pipeline.stream.IterableGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class PipelineStreamInterruptionTest
{
    @Test
    void testStreamInterruptAlways() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .interrupt()
            .sink(i -> assertion.activate())
        ;

        Flow.runAsPipeline(flow);
        Assertions.assertFalse(assertion.isActivated());
        Assertions.assertEquals(0, assertion.getActivationCount());
    }

    @Test
    void testStreamInterruptIf() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .interruptIf(i -> i <= 3)
            .sink(i -> assertion.activate())
        ;

        Flow.runAsPipeline(flow);
        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(3, assertion.getActivationCount());
    }

    @Test
    void testStreamInterruptIfAccumulate() throws RunException, CompilationException
    {
        var flow = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .interruptIf(i -> i <= 3)
            .accumulate(Collection::size)
            .collect()
        ;

        var output = Flow.runAsPipeline(flow);

        Assertions.assertEquals(3, output.get(flow));
    }
}
