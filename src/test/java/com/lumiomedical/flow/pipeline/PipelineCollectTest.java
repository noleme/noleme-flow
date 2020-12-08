package com.lumiomedical.flow.pipeline;

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
        var recipient = FlowDealer.joinSub(
            FlowDealer.joinMult(
                FlowDealer.sourceReturns8(),
                FlowDealer.sourceReturns9()
            ),
            FlowDealer.sourceReturns6()
        ).collect();

        Flow.runAsPipeline(recipient);

        Assertions.assertEquals(66, recipient.getContent());
    }

    @Test
    void testCollect2() throws RunException, CompilationException
    {
        var recipient = FlowDealer.sourceReturns8()
            .into(i -> i * 3)
            .join(FlowDealer.sourceReturns9(), (a, b) -> a - b)
            .join(FlowDealer.sourceReturns6(), (a, b) -> a ^ b)
            .join(FlowDealer.sourceReturns8(), (a, b) -> ((float)a / (float)b))
            .collect()
        ;

        Flow.runAsPipeline(recipient);

        Assertions.assertEquals(1.125F, recipient.getContent());
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

        var runtime = Flow.runAsPipeline(flow);

        Assertions.assertEquals(15, runtime.getSample("a", Integer.class));
        Assertions.assertEquals(9, runtime.getSample("b", Integer.class));
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

        var runtime = Flow.runAsPipeline(flow);

        Assertions.assertThrows(RunException.class, () -> runtime.getSample("a", Double.class));
    }
}
