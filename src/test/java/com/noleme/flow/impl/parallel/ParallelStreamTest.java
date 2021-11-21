package com.noleme.flow.impl.parallel;

import com.noleme.flow.Flow;
import com.noleme.flow.FlowAssertion;
import com.noleme.flow.actor.accumulator.AccumulationException;
import com.noleme.flow.actor.generator.IntegerGenerator;
import com.noleme.flow.actor.generator.LongGenerator;
import com.noleme.flow.compiler.CompilationException;
import com.noleme.flow.compiler.RunException;
import com.noleme.flow.impl.pipeline.stream.IterableGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class ParallelStreamTest
{
    private static final Logger logger = LoggerFactory.getLogger(ParallelStreamTest.class);

    @Test
    void testStream() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .sink(i -> assertion.activate())
        ;

        Flow.runAsParallel(flow);

        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(5, assertion.getActivationCount());
    }

    @Test
    void testSingleItemStream() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .from(() -> List.of(1))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .sink(i -> {
                Assertions.assertEquals(2, i);
                assertion.activate();
            })
        ;

        Flow.runAsParallel(flow);

        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(1, assertion.getActivationCount());
    }

    @Test
    void testAccumulation() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flow = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .accumulate(ls -> ls.stream()
                .reduce(Integer::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            )
            .driftSink(i -> assertion.activate()).asFlow()
            .collect()
        ;

        var output = Flow.runAsParallel(flow);

        Assertions.assertEquals(20, output.get(flow));
        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(1, assertion.getActivationCount());
    }

    @Test
    void testAccumulationOnEmptyStream() throws RunException, CompilationException
    {
        var flow = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .accumulate(ls -> ls.stream()
                .reduce(Integer::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            ).asFlow()
            .collect()
        ;

        var output = Flow.runAsParallel(flow);

        Assertions.assertEquals(15, output.get(flow));
    }

    @Test
    void testStreamJoinSmall() throws RunException, CompilationException
    {
        var flowA = Flow
            .from(() -> 3)
            .into(i -> i + 2)
        ;

        var flow = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .join(flowA, (current, a) -> current * a)
            .accumulate(ls -> ls.stream()
                .reduce(Integer::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            ).asFlow()
            .collect()
        ;

        var output = Flow.runAsParallel(flow);

        Assertions.assertEquals(100, output.get(flow));
    }

    @Test
    void testStreamJoinLarge() throws RunException, CompilationException
    {
        var flowA = Flow
            .from(() -> 3)
            .into(i -> i + 2)
        ;

        var flowB = Flow
            .from(() -> 2)
            .into(i -> i * 3)
        ;

        var flow = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .join(flowA, (current, a) -> current * a)
            .into(i -> i - 1)
            .accumulate(ls -> ls.stream()
                .reduce(Integer::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            )
            .join(flowB, (current, b) -> current * b).asFlow()
            .collect()
        ;

        var output = Flow.runAsParallel(flow);

        Assertions.assertEquals(570, output.get(flow));
    }

    @Test
    void testStreamGenerator() throws RunException, CompilationException
    {
        /* We generate integers starting from 1 and multiplying the output by 3 until we reach 100M */
        var flow = Flow
            .stream(() -> new IntegerGenerator(1, 100_000_000, i -> i * 3))
            .into(i -> i + 1)
            .accumulate(Collection::size).asFlow()
            .collect()
        ;

        var output = Flow.runAsParallel(flow);

        Assertions.assertEquals(17, output.get(flow));
    }

    @Test
    void testStreamGeneratorWithJoin() throws RunException, CompilationException
    {
        var flowA = Flow
            .stream(() -> new IntegerGenerator(1, 500_000_000, i -> i * 3))
            .into(i -> i + 1)
            .accumulate(Collection::size)
        ;

        var flowB = Flow
            .stream(() -> new IntegerGenerator(1, 500_000_000, i -> i * 2)).setMaxParallelism(2)
            .into(i -> i + 2)
            .accumulate(Collection::size).asFlow()
        ;

        var flow = flowA.join(flowB, Integer::sum).asFlow()
            .collect()
        ;

        var output = Flow.runAsParallel(flow);

        Assertions.assertEquals(48, output.get(flow));
    }

    @Test
    void testStreamGeneratorWithJoin2() throws RunException, CompilationException
    {
        var flowA = Flow
            .stream(() -> new IntegerGenerator(1, 500_000_000, i -> i * 3)).setMaxParallelism(2)
            .into(i -> i + 1)
            .accumulate(Collection::size).asFlow()
        ;

        var flowB = Flow
            .stream(() -> new IntegerGenerator(1, 500_000_000, i -> i * 2)).setMaxParallelism(2)
            .into(i -> i + 2)
            .join(flowA, (b, a) -> a * b)
            .accumulate(ls -> ls.stream()
                .reduce(Integer::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            ).asFlow()
            .collect()
        ;

        var output = Flow.runAsParallel(flowB);

        Assertions.assertEquals(1610613819, output.get(flowB));
    }

    @Test
    void testStreamGeneratorWithJoin3() throws RunException, CompilationException
    {
        var flowA = Flow
            .stream(() -> new LongGenerator(1, 500_000_000L, i -> i * 3)).setMaxParallelism(2)
            .into(i -> i + 1)
            .accumulate(Collection::size).asFlow()
        ;

        var flowB = Flow
            .stream(() -> new LongGenerator(1, 500_000_000, i -> i * 2)).setMaxParallelism(3)
            .into(i -> i + 2)
            .join(flowA, (b, a) -> a * b)
            .accumulate(ls -> ls.stream()
                .reduce(Long::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            ).asFlow()
        ;

        var flow = flowA.join(flowB, Long::sum)
            .collect()
        ;

        var output = Flow.runAsParallel(flow);

        Assertions.assertEquals(10_200_548_430L, output.get(flow));
    }

    @Test
    void testStreamAfterFlow() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flowA = Flow
            .from(() -> 3)
            .into(i -> i * 4)
            .driftSink(i -> assertion.activate())
        ;

        var flowB = Flow
            .from(() -> List.of(6, 7, 8, 9))
            .stream(IterableGenerator::new)
            .into(i -> i + 2)
            .driftSink(i -> assertion.activate())
            .accumulate(Collection::size).asFlow()
            .collect()
        ;

        Flow.sources(flowB).forEach(s -> s.after(flowA));

        var output = Flow.runAsParallel(flowA, flowB);

        Assertions.assertEquals(4, output.get(flowB));
        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(5, assertion.getActivationCount());
    }

    @Test
    void testStreamAfterStream() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flowA = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .driftSink(i -> assertion.activate())
            .accumulate(ls -> ls.stream()
                .reduce(Integer::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            ).asFlow()
        ;

        var flowB = Flow
            .from(() -> List.of(6, 7, 8, 9))
            .stream(IterableGenerator::new)
            .into(i -> i + 2)
            .driftSink(i -> assertion.activate())
            .accumulate(ls -> ls.stream()
                .reduce(Integer::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            ).asFlow()
        ;

        var flow = flowA
            .join(flowB, Integer::sum)
            .collect()
        ;

        Flow.sources(flowB).forEach(s -> s.after(flowA));

        var output = Flow.runAsParallel(flow);

        Assertions.assertEquals(58, output.get(flow));
        Assertions.assertTrue(assertion.isActivated());
        Assertions.assertEquals(9, assertion.getActivationCount());
    }

    @Test
    void testFlowAfterStream() throws RunException, CompilationException
    {
        var assertion = new FlowAssertion();

        var flowA = Flow
            .from(() -> List.of(1, 2, 3))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .into(i -> {
                assertion.activate();
                return i;
            })
        ;

        var flowB = Flow
            .from(() -> 2)
            .into(i -> {
                Assertions.assertTrue(assertion.isActivated());
            })
        ;

        flowB.after(flowA);

        var flow = flowA
            .into(i -> i * 3)
            .accumulate(ls -> ls.stream()
                .reduce(Integer::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            ).asFlow()
            .collect()
        ;

        var output = Flow.runAsParallel(flow, flowB);

        Assertions.assertEquals(27, output.get(flow));
    }
}
