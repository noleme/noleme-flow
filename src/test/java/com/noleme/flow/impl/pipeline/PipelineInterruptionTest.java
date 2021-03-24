package com.noleme.flow.impl.pipeline;

import com.noleme.flow.Flow;
import com.noleme.flow.FlowDealer;
import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.compiler.RunException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class PipelineInterruptionTest
{
    @Test
    void testInterruptAlways() throws RunException, CompilationException
    {
        var flow = FlowDealer.sourceReturns8()
            .into(i -> i * 3)
            .join(FlowDealer.sourceReturns9(), (a, b) -> a - b)
            .sample("before_interruption")
            .interrupt()
            .into(i -> i * 3)
            .collect()
        ;

        var output = Flow.runAsPipeline(flow);

        Assertions.assertEquals(15, output.get("before_interruption", Integer.class));
        Assertions.assertNull(output.get(flow));
    }

    @Test
    void testInterruptNotPassing() throws RunException, CompilationException
    {
        var flow = FlowDealer.sourceReturns8()
            .into(i -> i * 3)
            .join(FlowDealer.sourceReturns9(), (a, b) -> a - b)
            .interruptIf(i -> i != 15)
            .into(i -> i * 3)
            .collect()
        ;

        var output = Flow.runAsPipeline(flow);

        Assertions.assertEquals(45, output.get(flow));
    }

    @Test
    void testInterruptPassing() throws RunException, CompilationException
    {
        var flow = FlowDealer.sourceReturns8()
            .into(i -> i * 3)
            .join(FlowDealer.sourceReturns9(), (a, b) -> a - b)
            .interruptIf(i -> i == 15)
            .into(i -> i * 3)
            .collect()
        ;

        var output = Flow.runAsPipeline(flow);

        Assertions.assertNull(output.get(flow));
    }
}
