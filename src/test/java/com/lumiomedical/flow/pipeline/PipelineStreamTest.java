package com.lumiomedical.flow.pipeline;

import com.lumiomedical.flow.Flow;
import com.lumiomedical.flow.actor.accumulator.AccumulationException;
import com.lumiomedical.flow.compiler.CompilationException;
import com.lumiomedical.flow.compiler.RunException;
import com.lumiomedical.flow.pipeline.stream.IterableGenerator;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author Pierre Lecerf (plecerf@lumiomedical.com)
 * Created on 2020/12/02
 */
public class PipelineStreamTest
{
    @Test
    void test1() throws RunException, CompilationException
    {
        var flow = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .sink(System.out::println)
        ;

        Flow.runAsPipeline(flow);
    }

    @Test
    void test2() throws RunException, CompilationException
    {
        var flowA = Flow
            .from(() -> 3)
            .into(i -> i + 2)
        ;

        var flowB = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .join(flowA, (b, a) -> a * b)
            .sink(System.out::println)
        ;

        Flow.runAsPipeline(flowB);
    }

    @Test
    void test3() throws RunException, CompilationException
    {
        var flowB = Flow
            .from(() -> 3)
            .into(i -> i + 2)
        ;

        var flowC = Flow
            .from(() -> 2)
            .into(i -> i * 3)
        ;

        var flowA = Flow
            .from(() -> List.of(1, 2, 3, 4, 5))
            .stream(IterableGenerator::new)
            .into(i -> i + 1)
            .join(flowB, (a, b) -> a * b)
            .into(i -> i - 1)
            .drift(System.out::println)
            .accumulate(ls -> ls.stream()
                .reduce(Integer::sum)
                .orElseThrow(() -> new AccumulationException("Could not sum stream data."))
            )
            .join(flowC, (a, c) -> a * c)
            .sink(System.out::println)
        ;

        Flow.runAsPipeline(flowA);
    }
}
