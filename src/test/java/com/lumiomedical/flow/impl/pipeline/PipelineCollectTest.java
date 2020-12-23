package com.lumiomedical.flow.impl.pipeline;

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
public class PipelineCollectTest
{
    @Test
    void testCollect1() throws RunException, CompilationException
    {
        var flow = FlowDealer.joinSub(
            FlowDealer.joinMult(
                FlowDealer.sourceReturns8(),
                FlowDealer.sourceReturns9()
            ),
            FlowDealer.sourceReturns6()
        ).collect();

        var output = new PipelineCompiler().compile(flow).run();

        Assertions.assertEquals(66, output.get(flow));
    }

    @Test
    void testCollect2() throws RunException, CompilationException
    {
        var flow = FlowDealer.sourceReturns8()
            .into(i -> i * 3)
            .join(FlowDealer.sourceReturns9(), (a, b) -> a - b)
            .join(FlowDealer.sourceReturns6(), (a, b) -> a ^ b)
            .join(FlowDealer.sourceReturns8(), (a, b) -> ((float)a / (float)b))
            .collect()
        ;

        var output = Flow.runAsPipeline(flow);

        Assertions.assertEquals(1.125F, output.get(flow));
    }

    @Test
    void testSample1() throws RunException, CompilationException
    {
        var flow = FlowDealer.sourceReturns8()
            .into(i -> i * 3)
            .join(FlowDealer.sourceReturns9(), (a, b) -> a - b)
            .sample("a")
            .join(FlowDealer.sourceReturns6(), (a, b) -> a ^ b)
            .sample("b")
            .join(FlowDealer.sourceReturns8(), (a, b) -> ((float)a / (float)b))
        ;

        var output = Flow.runAsPipeline(flow);

        Assertions.assertEquals(15, output.get("a", Integer.class));
        Assertions.assertEquals(9, output.get("b", Integer.class));
    }

    @Test
    void testSampleError1() throws RunException, CompilationException
    {
        var flow = FlowDealer.sourceReturns8()
            .into(i -> i * 3)
            .join(FlowDealer.sourceReturns9(), (a, b) -> a - b)
            .sample("a")
            .join(FlowDealer.sourceReturns6(), (a, b) -> a ^ b)
        ;

        var output = Flow.runAsPipeline(flow);

        Assertions.assertThrows(ClassCastException.class, () -> output.get("a", Double.class));
    }
}
