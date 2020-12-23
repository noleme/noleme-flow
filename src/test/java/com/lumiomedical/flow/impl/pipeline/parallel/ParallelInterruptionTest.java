package com.lumiomedical.flow.impl.pipeline.parallel;

import com.lumiomedical.flow.Flow;
import com.lumiomedical.flow.FlowDealer;
import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.RunException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class ParallelInterruptionTest
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

        var runtime = Flow.runAsParallel(flow);

        Assertions.assertEquals(15, runtime.getSample("before_interruption", Integer.class));
        Assertions.assertNull(flow.getContent());
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

        Flow.runAsParallel(flow);

        Assertions.assertEquals(45, flow.getContent());
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

        Flow.runAsParallel(flow);

        Assertions.assertNull(flow.getContent());
    }
}
