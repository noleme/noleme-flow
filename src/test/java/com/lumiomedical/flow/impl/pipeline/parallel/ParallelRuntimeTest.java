package com.lumiomedical.flow.impl.pipeline.parallel;

import com.lumiomedical.flow.Flow;
import com.lumiomedical.flow.FlowAssertion;
import com.lumiomedical.flow.FlowDealer;
import com.lumiomedical.flow.FlowState;
import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.RunException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/03/10
 */
public class ParallelRuntimeTest
{
    @Test
    void testSimpleStringEdit() throws RunException, CompilationException
    {
        var pipeAssertion = new FlowAssertion();

        var pipe = Flow.from(() -> "This is my string.")
            .into((s) -> {
                return s.replace(" is ", " is not ");
            })
            .into((value) -> {
                Assertions.assertEquals("This is not my string.", value);
                pipeAssertion.activate();
            });

        Flow.runAsParallel(pipe);
        Assertions.assertTrue(pipeAssertion.isActivated());
    }

    @Test
    void testSimpleArithmetic1() throws RunException, CompilationException
    {
        var pipeAssertion = new FlowAssertion();

        var pipe = FlowDealer.sourceReturns8().into((value) -> {
            Assertions.assertEquals(8, value);
            pipeAssertion.activate();
        });

        Flow.runAsParallel(pipe);
        Assertions.assertTrue(pipeAssertion.isActivated());
    }

    @Test
    void testSimpleArithmetic2() throws RunException, CompilationException
    {
        var pipeAssertion = new FlowAssertion();

        var pipe = FlowDealer.sourceReturns9().into((value) -> {
            Assertions.assertEquals(9, value);
            pipeAssertion.activate();
        });

        Flow.runAsParallel(pipe);
        Assertions.assertTrue(pipeAssertion.isActivated());
    }

    @Test
    void testSimpleArithmetic3() throws RunException, CompilationException
    {
        var pipeAssertion = new FlowAssertion();

        var pipe = FlowDealer.sourceReturns6().into((value) -> {
            Assertions.assertEquals(6, value);
            pipeAssertion.activate();
        });

        Flow.runAsParallel(pipe);
        Assertions.assertTrue(pipeAssertion.isActivated());
    }

    @Test
    void testSimpleArithmetic4() throws RunException, CompilationException
    {
        var pipeAssertion1 = new FlowAssertion();
        var pipeAssertion2 = new FlowAssertion();

        var pipe = FlowDealer.sourceReturns6();

        var sink1 = pipe.into((value) -> {
            Assertions.assertEquals(6, value);
            pipeAssertion1.activate();
        });
        var sink2 = pipe
            .into((value) -> value * 3)
            .into((value) -> {
                Assertions.assertEquals(18, value);
                pipeAssertion2.activate();
            });

        Flow.runAsParallel(sink1, sink2);
        Assertions.assertTrue(pipeAssertion1.isActivated());
        Assertions.assertTrue(pipeAssertion2.isActivated());
    }

    @Test
    void testSimpleArithmetic5() throws RunException, CompilationException
    {
        var pipeAssertion1 = new FlowAssertion();
        var pipeAssertion2 = new FlowAssertion();

        var pipe = FlowDealer.sourceReturns6();

        var sink1 = pipe.into((value) -> {
            Assertions.assertEquals(6, value);
            Assertions.assertTrue(pipeAssertion1.isActivated());
            pipeAssertion2.activate();
        });
        var sink2 = pipe
            .into((value) -> value * 3)
            .into((value) -> {
                Assertions.assertEquals(18, value);
                pipeAssertion1.activate();
            });

        sink1.after(sink2);

        Flow.runAsParallel(sink1, sink2);
        Assertions.assertTrue(pipeAssertion2.isActivated());
    }

    @Test
    void testSimpleJoin1() throws RunException, CompilationException
    {
        var pipeAssertion = new FlowAssertion();

        var pipe = FlowDealer.joinSub(
            FlowDealer.sourceReturns8(),
            FlowDealer.sourceReturns9()
        ).into((value) -> {
            Assertions.assertEquals(-1, value);
            pipeAssertion.activate();
        });

        Flow.runAsParallel(pipe);
        Assertions.assertTrue(pipeAssertion.isActivated());
    }

    @Test
    void testSimpleJoin2() throws RunException, CompilationException
    {
        var pipeAssertion = new FlowAssertion();

        var pipe = FlowDealer.joinSub(
            FlowDealer.joinMult(
                FlowDealer.sourceReturns8(),
                FlowDealer.sourceReturns9()
            ),
            FlowDealer.sourceReturns6()
        ).into((value) -> {
            Assertions.assertEquals(66, value);
            pipeAssertion.activate();
        });

        Flow.runAsParallel(pipe);
        Assertions.assertTrue(pipeAssertion.isActivated());
    }

    @Test
    void testDrift1() throws RunException, CompilationException
    {
        FlowState<Integer> stateA = new FlowState<>();
        FlowState<Integer> stateB = new FlowState<>();
        FlowState<Integer> stateC = new FlowState<>();

        var flow = Flow.from(() -> 10)
            .into(i -> i * 2)
            .driftSink(stateA::setValue)
            .into(i -> i / 4)
            .driftSink(stateB::setValue)
            .into(i -> i * 2)
            .into(stateC::setValue)
        ;

        Flow.runAsParallel(flow);

        Assertions.assertEquals(20, stateA.getValue());
        Assertions.assertEquals(5, stateB.getValue());
        Assertions.assertEquals(10, stateC.getValue());
    }
}
